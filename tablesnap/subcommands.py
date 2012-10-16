"""Base for commands"""

import argparse
import errno
import logging
import os
import os.path
import Queue
import signal
import threading
import time

from watchdog import events, observers

import file_util

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
    def _common_args(cls):
        """Returns a :cls:`argparser.ArgumentParser` with the common 
        arguments for this Command hierarchy.

        Common Args are used when adding a sub parser for a sub command.

        Sub classes may override this method but should pass the call up 
        to ensure the object is correctly created.
        """
        return argparse.ArgumentParser(add_help=False, 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

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
            parents=[cls._common_args()],
            help=cls.command_help or "No help", 
            description=cls.command_description or "No help", 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.set_defaults(func=cls)
        return parser

    def __call__(self):
        """Called to execute the SubCommand.
        
        Must be implemented by sub classes.
        """
        raise NotImplementedError()

# ============================================================================
# Snap - used to backup files
        
class SnapSubCommand(SubCommand):
    """Base for Sub Commands that watch and backup files. 
    """

    @classmethod
    def _common_args(cls):
        """Returns a :class:`argparser.ArgumentParser` with the common 
        arguments for this Command hierarchy.
        """

        common_args = super(SnapSubCommand, cls)._common_args()

        common_args.add_argument("--threads", type=int, default=4,
            help='Number of writer threads.')
        common_args.add_argument('--recursive', action='store_true', 
            default=False,
            help='Recursively watch the given path(s)s for new SSTables')

        common_args.add_argument('--exclude-keyspace', 
            dest='exclude_keyspaces', nargs="*",
            help="User keyspaces to exclude from backup.")
        common_args.add_argument('--include-system-keyspace', default=False,
            dest='include_system_keyspace', action="store_true",
            help="Include the system keyspace.")

        common_args.add_argument('--ignore-existing', default=False,
            dest='ignore_existing', action="store_true",
            help="Don't backup existing files.")
        common_args.add_argument('--ignore-changes', default=False,
            dest='ignore_changes', action="store_true",
            help="Don't watch for file changes, exit immediately.")

        common_args.add_argument('--test_mode', 
            action='store_true', dest='test_mode', default=False,
            help="Do not upload .")

        common_args.add_argument("cassandra_data_dir", 
            help="Top level Cassandra data directory, normally "\
            "/var/lib/cassandra/data.")

        return common_args

    def __init__(self, args):
        self.args = args
        return

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
            self._create_worker_thread(i, file_q, self.args)
            for i in range(self.args.threads)
        ]
        for worker in self.workers:
            worker.start()

        # Start the watcher
        watcher.start()

        self.log.info("Finished sub command %s" % self.command_name)
        return

    def _create_worker_thread(self, i, file_queue, args):
        """Called to create an endpoint to be used with a worker thread.

        Sublcasses must implement this.
        """
        raise NotImplementedError()

class SnapWorkerThread(threading.Thread):
    log = logging.getLogger("%s.%s" % (__name__, "SnapWorkerThread"))

    def __init__(self, thread_id, file_q, args):
        super(SnapWorkerThread, self).__init__()

        self.name = "SnapWorker-%(thread_id)s" % vars()
        self.daemon = True

        self.file_q = file_q
        self.args = args

    def run(self):
        """Wait to get work from the :attr:`file_q`
        """

        while True:
            # blocking
            ks_manifest, cass_file = self.file_q.get()
            self.log.info("Uploading file %(cass_file)s" % vars())
            try:

                self._store(ks_manifest, cass_file)
            except (EnvironmentError) as e:
                # sometimes it's an IOError sometimes OSError
                # EnvironmentError is the base
                if not(e.errno == errno.ENOENT and \
                    e.filename==cass_file.file_path):
                    raise
                self.log.info("Aborted uploading %(cass_file)s was removed" %\
                    vars())

            except (Exception):
                self.log.critical("Failed uploading %s. Aborting." %
                    (cass_file,), exc_info=True)
                # Brute force kill self
                os.kill(os.getpid(), signal.SIGKILL)
            else:
                self.log.info("Uploaded file %(cass_file)s" % vars())

            self.file_q.task_done()
        return

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

        try:
            cass_file = file_util.CassandraFile.from_file_path(file_path)
        except (ValueError):
            self.log.info("Ignoring non Cassandra file %(file_path)s" % \
                vars())
            return False

        if not cass_file.should_backup():
            self.log.info("Ignoring file %(cass_file)s" % vars())
            return False

        if cass_file.descriptor.ks_name in self.exclude_keyspaces:
            self.log.info("Ignoring file %s from excluded "\
                "keyspace %s" % (cass_file, cass_file.descriptor.ks_name))
            return False

        if (cass_file.descriptor.ks_name.lower() == "system") and (
            not self.include_system_keyspace):

            self.log.info("Ignoring system keyspace file %(cass_file)s"\
                % vars())
            return False

        ks_manifest = file_util.KeyspaceManifest.from_dir(
            self.data_dir, cass_file.descriptor.ks_name)
        self.log.info("Queueing file %(cass_file)s"\
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

    @classmethod
    def _common_args(cls):
        """Returns a :class:`argparser.ArgumentParser` with the common 
        arguments for this Command hierarchy.
        """

        common_args = super(ListSubCommand, cls)._common_args()
        common_args.add_argument('--all', 
            action='store_true', dest='list_all', default=False,
            help="List all backups that match the criteria. Otherwise only "\
            "the most recent backup is listed.")

        common_args.add_argument("keyspace",
            help="Keyspace to list backups from.")
        common_args.add_argument('host',  
            help="Host to list backups from.")

        return common_args

    def __init__(self, args):
        self.args = args
        

    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        manifests = self._list_manifests()

        buffer = ["All" if self.args.list_all else "Latest" + \
            " backups for keyspace %(keyspace)s from %(host)s:" % vars(
            self.args)]
        for f in manifests:
            name, _ = os.path.splitext(f)
            buffer.append(name)

        self.log.info("Finished sub command %s" % self.command_name)
        return "\n".join(buffer) 

    def _list_manifests(self, keyspace, host, list_all):
        """Called to create an endpoint to be used with a worker thread.

        Sublcasses must implement this.
        """
        raise NotImplementedError()


