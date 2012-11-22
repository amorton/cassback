"""Base for commands"""

import argparse
import copy
import errno
import logging
import os
import os.path
import Queue
import socket
import signal
import sys
import threading
import time
import traceback

from watchdog import events, observers

import cassandra, endpoints, dt_util, file_util

# ============================================================================
# 

class SubCommand(object):
    """Base for all SubCommands that can be called on the command line. 

    :cls:`SubCommand` instances are created by the script entry point and 
    their ``__call__`` method called. 
    """

    command_name = None
    """Command line name for the Sub Command.

    Must be specified by sub classes.
    """

    command_help = None
    """Command line help for the Sub Command."""

    command_description = None
    """Command line description for the Sub Command."""


    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 

        Sub classes may override this method but pass the call up to ensure 
        the sub parser is created correctly. 

        A default ``func`` argument is set on the :cls:``ArgumentParser`` to 
        point to the constructor for the SubCommand class.

        Returns the :cls:`argparser.ArgumentParser`.
        """
        
        assert cls.command_name, "command_name must be set."

        parser = sub_parsers.add_parser(cls.command_name,
            help=cls.command_help or "No help", 
            description=cls.command_description or "No help", 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.set_defaults(func=cls)
        return parser
    
    def __call__(self):
        """Called to execute the SubCommand.
        
        Must return a tuple of (rv, msg). Rv is returned as the script exit 
        and msg is the std out message.
        
        Must be implemented by sub classes.
        """
        raise NotImplementedError()
    
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Utility Functions
    
    def _endpoint(self, args):
        """Creates an endpoint from the command args."""
        return endpoints.create_from_args(args)
        
    def _list_manifests(self, endpoint, keyspace, host):
        """List all the manifests available for the ``keyspace`` and 
        ``host`` using the ``endpoint``.
        
        Returns a list of the file names.
        """
        
        manifest_dir = cassandra.KeyspaceManifest.backup_dir(keyspace) 

        host_manifests = []
        for file_name in endpoint.iter_dir(manifest_dir):
            backup_name, _ = os.path.splitext(file_name)
            manifest = cassandra.KeyspaceManifest.from_backup_name(
                backup_name)
            if manifest.host == host:
                host_manifests.append(file_name)
        return host_manifests
        
    def _load_manifest(self, endpoint, backup_name):
        """Load the :cls:`cassandra.KeyspaceManifest` for the 
        backup with ``backup_name`` using the ``endpoint``.
        """

        empty_manifest = cassandra.KeyspaceManifest.from_backup_name(
            backup_name)
        manifest_data = endpoint.read_json(empty_manifest.backup_path)
        return cassandra.KeyspaceManifest.from_manifest(manifest_data)

class SubCommandWorkerThread(threading.Thread):
    """Base for threads used by sub commands.
    
    Provides a top level exception handler for the thread that kills the 
    process if :attr:`kill_on_error` is set. 
    
    Sub classes should implement :func:`_do_run` rather than :func:`run`.
    """
    
    def __init__(self, name, thread_id):
        super(SubCommandWorkerThread, self).__init__()

        self.name = "%(name)s-%(thread_id)s" % vars()
        self.daemon = True

        self.kill_on_error = True
        assert self.log, "Must have logger"
    
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Thread overrides.
    
    def run(self):

        try:
            self._do_run()
        except (Exception):
            msg = "Unexpected error in worker thread %s%s" % (
                self.name, " killing process" if self.kill_on_error else "")

            print msg
            print traceback.format_exc()
            self.log.critical(msg, exc_info=True)

            if self.kill_on_error:
                # Brute force kill self
                os.kill(os.getpid(), signal.SIGKILL)
    
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Abstract Methods
    
    def _do_run():
        """Called by :func:`run` to start the thread. 
        """
        pass

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # utility Methods
    
    def _endpoint(self, args):
        """Creates an endpoint from the command args."""
        return endpoints.create_from_args(args)
        
# ============================================================================
# Snap - used to backup files
        
class SnapSubCommand(SubCommand):
    log = logging.getLogger("%s.%s" % (__name__, "SnapSubCommand"))

    command_name = "snap"
    command_help = "Backup SSTables"
    command_description = "backup SSTables"

    def __init__(self, args):
        self.args = args
        return

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Subcommand Overrides
    
    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """

        sub_parser = super(SnapSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("--threads", type=int, default=4,
            help='Number of writer threads.')
        sub_parser.add_argument('--recursive', action='store_true', 
            default=False,
            help='Recursively watch the given path(s)s for new SSTables')

        sub_parser.add_argument('--exclude-keyspace', 
            dest='exclude_keyspaces', nargs="*",
            help="User keyspaces to exclude from backup.")
        sub_parser.add_argument('--include-system-keyspace', default=False,
            dest='include_system_keyspace', action="store_true",
            help="Include the system keyspace.")

        sub_parser.add_argument('--ignore-existing', default=False,
            dest='ignore_existing', action="store_true",
            help="Don't backup existing files.")
        sub_parser.add_argument('--ignore-changes', default=False,
            dest='ignore_changes', action="store_true",
            help="Don't watch for file changes, exit immediately.")

        sub_parser.add_argument("--cassandra_data_dir", 
            default="/var/lib/cassandra/data",
            help="Top level Cassandra data directory.")

        return sub_parser

    def __call__(self):
        self.log.info("Starting sub command %s" % self.command_name)
        
        # Make a queue, we put the files that need to be backed up here.
        file_q = Queue.Queue()

        # Make a watcher
        watcher = WatchdogWatcher(self.args.cassandra_data_dir, file_q, 
            self.args.ignore_existing, self.args.ignore_changes, 
            self.args.exclude_keyspaces, self.args.include_system_keyspace)

        # Make worker threads
        self.workers = [
            self._create_worker_thread(i, file_q)
            for i in range(self.args.threads)
        ]
        for worker in self.workers:
            worker.start()

        # Start the watcher
        watcher.start()

        self.log.info("Finished sub command %s" % self.command_name)
        
        # There is no message to call. Assume the process has been running 
        # for a while.
        return (0, "")

    def _create_worker_thread(self, i, file_queue):
        """Creates a worker thread for the snap command
        """
        return SnapWorkerThread(i, file_queue, copy.copy(self.args))

class SnapWorkerThread(SubCommandWorkerThread):
    log = logging.getLogger("%s.%s" % (__name__, "SnapWorkerThread"))

    def __init__(self, thread_id, file_q, args):
        super(SnapWorkerThread, self).__init__("SnapWorker-", thread_id)

        self.file_q = file_q
        self.args = args

    def _do_run(self):
        """Wait to get work from the :attr:`file_q`
        """

        endpoint = self._endpoint(self.args)
        while True:
            # blocking call
            ks_manifest, cass_file = self.file_q.get()
            try:
                self._run_internal(endpoint, ks_manifest, cass_file)
            except (EnvironmentError) as e:
                # sometimes it's an IOError sometimes OSError
                # EnvironmentError is the base
                if not(e.errno == errno.ENOENT and \
                    e.filename==cass_file.original_path):
                    raise
                self.log.info("Aborted uploading %(cass_file)s was removed" %\
                    vars())
            self.file_q.task_done()
        return

    def _run_internal(self, endpoint, ks_manifest, cass_file):
        """Backup the cassandra file and keyspace manifest. 

        Let errors from here buble out. 
        
        Returns `True` if the file was uploaded, `False` otherwise.
        """

        self.log.info("Uploading file %s" % (cass_file.original_path,))
            
        # Store the cassandra file
        if endpoint.exists(cass_file.backup_path):
            if endpoint.validate_checksum(cass_file.backup_path, 
                cass_file.meta["md5_hex"]):
                
                self.log.info("Skipping file %s skipping as there is a "\
                    "valid backup"% (cass_file.original_path,))
            else:
                self.log.warn("Possibly corrupt file %s in the backup at %s"\
                    " skipping." % (cass_file.original_path, full_path))
            return False

        uploaded_path = endpoint.store_with_meta(cass_file.original_path,
            cass_file.meta, cass_file.backup_path)
        endpoint.store_json(ks_manifest.to_manifest(),
            ks_manifest.backup_path)
        
        self.log.info("Uploaded file %s to %s" % (cass_file.original_path, 
            uploaded_path))
        return True
        

class WatchdogWatcher(events.FileSystemEventHandler):
    """Watch the disk for new files."""
    log = logging.getLogger("%s.%s" % (__name__, "WatchdogWatcher"))

    def __init__(self, data_dir, file_queue, ignore_existing, ignore_changes, 
        exclude_keyspaces, include_system_keyspace):
        
        self.data_dir = data_dir
        self.file_queue = file_queue
        self.ignore_existing = ignore_existing
        self.ignore_changes = ignore_changes
        self.exclude_keyspaces = frozenset(exclude_keyspaces or [])
        self.include_system_keyspace = include_system_keyspace

    def start(self):

        if not self.ignore_existing:
            self.log.info("Refreshing existing files.")
            for root, dirs, files in os.walk(self.data_dir):
                for filename in files:
                    self._maybe_queue_file(os.path.join(root, filename))

        # watch if configured
        if self.ignore_changes:
            return 

        observer = observers.Observer()
        observer.schedule(self, path=self.data_dir, recursive=True)
        self.log.info("Watching for new file under %(data_dir)s." %\
            vars(self))

        observer.start()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            observer.stop()
        observer.join(timeout=30)
        if observer.isAlive():
            self.log.error("Watchdog Observer failed to stop. Aborting.")
            os.kill(os.getpid(), signal.SIGKILL)
        return

    def _maybe_queue_file(self, file_path):

        if cassandra.is_snapshot_path(file_path):
            self.log.info("Ignoring snapshot path %(file_path)s" % vars())
            return False

        try:
            cass_file = cassandra.CassandraFile.from_file_path(file_path)
        except (ValueError):
            self.log.info("Ignoring non Cassandra file %(file_path)s" % \
                vars())
            return False
        except (EnvironmentError) as e:
            if e.errno == errno.ENOENT:
                self.log.info("Ignoring missing file %(file_path)s" % \
                    vars())
                return False
            else:
                raise

        if cass_file.descriptor.temporary:
            self.log.info("Ignoring temporary file %(file_path)s" % vars())
            return False

        if cass_file.descriptor.keyspace in self.exclude_keyspaces:
            self.log.info("Ignoring file %s from excluded "\
                "keyspace %s" % (cass_file, cass_file.descriptor.keyspace))
            return False

        if (cass_file.descriptor.keyspace.lower() == "system") and (
            not self.include_system_keyspace):

            self.log.info("Ignoring system keyspace file %(file_path)s"\
                % vars())
            return False

        ks_manifest = cassandra.KeyspaceManifest.from_cass_file(cass_file)
        self.log.info("Queueing file %(file_path)s"\
            % vars())
        self.file_queue.put((ks_manifest, cass_file))
        return True

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Watchdog file events.

    def on_created(self, event):
        self._maybe_queue_file(event.src_path)
        return

    def on_moved(self, event):
        self._maybe_queue_file(event.dest_path)
        return


# ============================================================================
# List - used to list backups

class ListSubCommand(SubCommand):
    """Base for Sub Commands that watch and backup files. 
    """

    log = logging.getLogger("%s.%s" % (__name__, "ListSubCommand"))

    command_name = "list"
    command_help = "List Backups"
    command_description = "List Backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """

        sub_parser = super(ListSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument('--all', 
            action='store_true', dest='list_all', default=False,
            help="List all backups that match the criteria. Otherwise only "\
            "the most recent backup is listed.")

        sub_parser.add_argument("keyspace",
            help="Keyspace to list backups from.")
        sub_parser.add_argument('--host',
            default=socket.getfqdn(),  
            help="Host to list backups from. Defaults to the current host.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        manifests = self._list_manifests()
        buffer = [("All backups" if self.args.list_all else "Latest backup")\
            + " for keyspace %(keyspace)s from %(host)s:" % vars(
            self.args)]

        for file_name in manifests:
            name, _ = os.path.splitext(file_name)
            buffer.append(name)

        self.log.info("Finished sub command %s" % self.command_name)
        return (0, "\n".join(buffer)) 

    def _list_manifests(self):
        
        endpoint = endpoints.create_from_args(self.args)
        manifest_dir = cassandra.KeyspaceManifest.backup_dir(
            self.args.keyspace) 

        host_manifests = []
        for file_name in endpoint.iter_dir(manifest_dir):
            backup_name, _ = os.path.splitext(file_name)
            manifest = cassandra.KeyspaceManifest.from_backup_name(
                backup_name)
            if manifest.host == self.args.host:
                host_manifests.append(file_name)

        if self.args.list_all:
            return host_manifests
        return [max(host_manifests),]


# ============================================================================
# Validate - validate that all files in a backup are present

class ValidateSubCommand(SubCommand):
    """Base for Sub Commands that watch and backup files. 
    """

    log = logging.getLogger("%s.%s" % (__name__, "ListSubCommand"))

    command_name = "validate"
    command_help = "Validate all files exist for a backup."
    command_description = "Validate all files exist for a backup."

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        sub_parser = super(ValidateSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("--checksum",
            action='store_true', dest='checksum', default=False,
            help="Do an MD5 checksum of the files.")

        sub_parser.add_argument('backup_name',  
            help="Backup to validate.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        endpoint = endpoints.create_from_args(self.args)
        manifest = self._load_manifest(endpoint, self.args.backup_name)

        missing_files = []
        present_files = []
        corrupt_files = []

        for file_name in manifest.yield_file_names():
            
            # Model the file in the manifest.
            cass_file = cassandra.CassandraFile.from_file_path(file_name, 
                meta={}, host=manifest.host)

            try:
                cass_file.meta = endpoint.read_meta(cass_file.backup_path)
            except (EnvironmentError) as e:
                if e == errno.ENOENT:
                    # missing file. 
                    cass_file = None    
                raise

            if cass_file is None:
                missing_files.append(file_name)
            elif endpoint.exists(cass_file.backup_path):
                if not self.args.checksum:
                    present_files.append(file_name)
                elif endpoint.validate_checksum(cass_file.backup_path, 
                    cass_file.meta["md5_hex"]):
                    present_files.append(file_name)
                else:
                    corrupt_files.append(file_name)
            else:
                missing_files.append(file_name)

        buffer = []
        if missing_files or corrupt_files:
            buffer.append("Missing or corrupt files found for backup "\
                "%s" % (self.args.backup_name,))
        else:
            buffer.append("All files present for backup %s"\
                 % (self.args.backup_name,))

        if self.args.checksum:
            buffer.append("Files were checksummed")
        else:
            buffer.append("Files were not checksummed")

        buffer.append("")

        if corrupt_files:
            buffer.append("Corrupt Files:")
            buffer.extend(corrupt_files)

        if missing_files:
            buffer.append("Missing Files:")
            buffer.extend(missing_files)

        if present_files:
            buffer.append("Files:")
            buffer.extend(present_files)

        self.log.info("Finished sub command %s" % self.command_name)
        return (
            0 if not missing_files and not corrupt_files else 1,
            "\n".join(buffer)
        )

# ============================================================================
# Slurp - restore a backup

class SlurpSubCommand(SubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "ListSubCommand"))

    command_name = "slurp"
    command_help = "Restore backups"
    command_description = "Restore backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        
        sub_parser = super(SlurpSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("--threads", type=int, default=1,
            help='Number of writer threads.')

        sub_parser.add_argument("cassandra_data_dir", 
            help="Top level Cassandra data directory, normally "\
            "/var/lib/cassandra/data.")

        sub_parser.add_argument('backup_name',  
            help="Backup to restore.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        endpoint = endpoints.create_from_args(self.args)
        manifest = self._load_manifest(endpoint, self.args.backup_name)

        # We put the files that need to be restored in there.
        work_queue = Queue.Queue()
        # We put the results in here
        # So we can say what was copied to where.
        result_queue = Queue.Queue()

        # Fill the queue with work to be done. 
        file_names = []
        for file_name in manifest.yield_file_names():
            work_queue.put(file_name)
            file_names.append(file_name)
        self.log.info("Queued file for restoring: %s" % (
            ", ".join(file_names)))

        # Make worker threads to do the work. 
        workers = [
            self._create_worker_thread(i, work_queue, result_queue)
            for i in range(self.args.threads)
        ]

        for worker in workers:
            worker.start()

        # Wait for the work queue to empty. 
        self.log.info("Waiting on workers.")
        work_queue.join()
        self.log.info("Finished sub command %s" % self.command_name)

        # Make a pretty message 
        buffer = ["Restored files:"]
        from_to = []
        while not result_queue.empty():
            from_to.append(result_queue.get_nowait())

        if from_to:      
            buffer.extend(
                "%s -> %s" % x
                for x in from_to
            )
        else:
            buffer.append("None")
        return (0, "\n".join(buffer))

    def _create_worker_thread(self, i, work_queue, result_queue):
        """Called to create an endpoint to be used with a worker thread.
        """
        return SlurpWorkerThread(i, work_queue, result_queue, 
            copy.copy(self.args))

class SlurpWorkerThread(SubCommandWorkerThread):
    log = logging.getLogger("%s.%s" % (__name__, "SlurpWorkerThread"))

    def __init__(self, thread_id, work_queue, result_queue, args):
        super(SlurpWorkerThread, self).__init__("SlurpWorker", thread_id)

        self.work_queue = work_queue
        self.result_queue = result_queue
        self.args = args

    def _do_run(self):
        """
        """

        def safe_get():
            try:
                return self.work_queue.get_nowait()
            except (Queue.Empty):
                return None

        endpoint = endpoints.create_from_args(self.args)
        file_name = safe_get()
        while file_name is not None:

            # Locate the file and work out where we are going to restore it 
            cass_file = cassandra.CassandraFile.from_file_path(file_name,
                meta={})
            dest_path = os.path.join(self.args.cassandra_data_dir, 
                cass_file.restore_path)
            self.log.info("Restoring file %(cass_file)s to %(dest_path)s" % \
                vars())

            # Restore the file if we want to
            should_restore, reason = self._should_restore(cass_file, 
                dest_path)
            if should_restore:
                endpoint.restore(cass_file.backup_path, dest_path)
                self.log.info("Restored file %(cass_file)s to %(dest_path)s"\
                     % vars())
                self.result_queue.put((file_name, dest_path))
            else:
                self.log.info("Skipping file %(cass_file)s because "\
                    "%(reason)s" % vars())
                self.result_queue.put((file_name, 
                    "Skipped %(reason)s" % vars()))

            self.work_queue.task_done()
            file_name = safe_get()
        return

    def _should_restore(self, cass_file, dest_path):
        """Called to test if the ``cass_file`` should be restored to 
        ``dest_path``.

        Returns a tuple of ``(should_restore, reason)`` where ``reason`` 
        is a string description to say why not, .e.g "Existing file"
        """

        if os.path.isfile(dest_path):
            return (False, "Existing file")

        return (True, None)

# ============================================================================
# Purge - remove files from the backup

class PurgeSubCommand(SubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "PurgeSubCommand"))

    command_name = "purge"
    command_help = "Purge old backups"
    command_description = "Purge old backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        sub_parser = super(PurgeSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("--keyspace", nargs="+",
            help="Keyspace to purge from, if not specified all keyspaces "\
            "are purged.")

        sub_parser.add_argument("--host",  
            help="Host to purge backups from, defaults to this host.")

        sub_parser.add_argument("--purge-before",
            dest='purge_before',
            help="Purge backups older than this date time.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):
        
        
        keyspaces = self.args.keyspace
        host = self.args.host or socket.getfqdn()
        purge_before = dt_util.parse_date_input(self.args.purge_before)
        
        endpoint = self._endpoint(self.args)
        
        # Step 1- get all the manifests
        self.log.debug("Building list of manifests.")
        all_manifests = self._all_manifests(endpoint, keyspaces, host)
        self.log.info("Candiate manifest count: %s" % (len(all_manifests)))
        
        # Step 2 - work out which manifests are staying and which are being 
        # purged
        kept_manifests = []
        purged_manifests = []

        for manifest in all_manifests:
            if manifest.timestamp < purge_before:
                purged_manifests.append(manifest)
            else:
                kept_manifests.append(manifest)
        self.log.info("Will purge %s backups and keep %s" % (
            len(purged_manifests), len(kept_manifests)))

        # Step 3 - build a set of the files we want to keep. 
        # We do this even if there are no purged manifests. A we could be 
        # cleaning up after a failed purge.
        kept_files = set()
        for manifest in kept_manifests:
            kept_files.update(manifest.yield_file_names())
        self.log.info("Keeping %s sstable files." % (len(kept_files)))

        # Step 4 - Purge the manifests
        deleted_files = self._purge_manifests(endpoint, purged_manifests)

        # Step 5 - Purge the files that are not referenced from a manifest.
        deleted_files.extend(self._purge_sstables(endpoint, keyspaces, host, 
            kept_files))

        buffer = ["Deleted files:", ""]
        if deleted_files:
            buffer.extend(deleted_files)
        else:
            buffer.append("No files")

        return (0, "\n".join(buffer))

    def _all_manifests(self, endpoint, keyspaces, host):
        """Loads all manifests for the ``keyspaces`` and ``host``.
        """
        
        # Step 1 - get the keyspace dirs
        # if keyspace arg is empty then get all. 
        if keyspaces:
            ks_dirs = [
                os.path.join("cluster", ks_name)
                for ks_name in keyspaces
            ]
        else:
            ks_dirs = list(
                os.path.join("cluster", d)
                for d in endpoint.iter_dir("cluster", include_files=False, 
                    include_dirs=True)
            )
            
        # Step 2 - Load the manifests
        manifests = []
        for ks_dir in ks_dirs:
            for manifest_file_name in endpoint.iter_dir(ks_dir):
                # Just load the manifest and check the host
                # could be better. 
                self.log.debug("Opening manifest file %(manifest_file_name)s"\
                    % vars())

                manifest = cassandra.KeyspaceManifest.from_manifest(
                    endpoint.read_json(os.path.join(ks_dir, 
                    manifest_file_name)))

                if manifest.host == host:
                    manifests.append(manifest)
        return manifests

    def _purge_manifests(self, endpoint,  purged_manifests):
        """Deletes the :cls:`cassandra.KeyspaceManifest` manifests 
        in ``purged_manifests``.
        
        Returns a list of the paths deleted. 
        """
        
        deleted = []
        for manifest in purged_manifests:
            path = manifest.backup_path
            self.log.info("Purging manifest file %(path)s" % vars())
            deleted.append(endpoint.remove_file(path))
        return deleted

    def _purge_sstables(self, endpoint, keyspaces, host, kept_files):
        """Deletes the sstables for in the ``keyspaces`` for the ``host``
        that are not listed in ``kept_files``. 
        
        If ``keyspaces`` is empty purge from all keyspaces.
        
        Returns a list of the paths deleted.
        """
        
        # Step 1 - work out the keyspace directores we want to delete from.
        if keyspaces:
            ks_dirs = [
                os.path.join("hosts", host, ks_name)
                for ks_name in keyspaces
            ]
        else:
            host_dir = os.path.join("hosts", host)
            ks_dirs = list(
                os.path.join(host_dir, d)
                for d in endpoint.iter_dir(host_dir, include_files=False, 
                    include_dirs=True)
            )

        # Step 3 - delete files not in the keep list.
        deleted_files = []
        for ks_dir in ks_dirs:
            for file_path in endpoint.iter_dir(ks_dir, recursive=True):
                _, file_name = os.path.split(file_path)

                if file_name in kept_files:
                    self.log.debug("Keeping file %(file_path)s" % vars())
                else:
                    self.log.debug("Deleting file %(file_path)s" % vars())
                    deleted_files.append(endpoint.remove_file_with_meta(
                        file_path))

        return deleted_files

# ============================================================================
# Show - show the contents of a backup

class ShowSubCommand(SubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "ShowSubCommand"))

    command_name = "show"
    command_help = "Show the contents of a backup."
    command_description = "Show the contents of a backup."

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        sub_parser = super(ShowSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("backup_name",
            help="Purge backups older than this date time.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):

        endpoint = self._endpoint(self.args)
        manifest = self._load_manifest(endpoint, self.args.backup_name)

        str_builder = ["Backup: %s" % manifest.backup_name]
        
        str_builder.append("")
        str_builder.append("Keyspace: %s:" % (manifest.keyspace,))
        str_builder.append("Host: %s:" % (manifest.host,))
        str_builder.append("Timestamp: %s:" % (manifest.timestamp,))
        
        for cf_name, cf_files in manifest.column_families.iteritems():
            str_builder.append("")
            str_builder.append("Column Family: %s" % (cf_name,))
            for cf_file in cf_files:
                str_builder.append("\t%s" % (cf_file,))
        
        return (0, "\n".join(str_builder))
        