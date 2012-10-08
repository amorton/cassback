"""Abstract Sub Commands for backing up Cassandra.

Notes:
Endpoints have store(), abort_store() retrieve()
"""

import errno
import logging
import os
import os.path
import Queue
import signal
import threading
import time

from watchdog import events, observers

import file_util, subcommands

class SnapConfig(object):
    """General config for storing files.
    """

    def __init__(self, cassandra_data_dir, threads, recursive, auto_add,
        test_mode, exclude_keyspaces, include_system_keyspace):
        self.cassandra_data_dir = cassandra_data_dir
        self.threads = threads
        self.recursive = recursive
        self.auto_add = auto_add
        self.test_mode = test_mode
        self.exclude_keyspaces = exclude_keyspaces
        self.include_system_keyspace = include_system_keyspace 

    @classmethod
    def from_args(cls, args):
        return SnapConfig(args.cassandra_data_dir, args.threads, args.recursive,
            args.auto_add, args.test_mode, 
            args.exclude_keyspace or [], args.include_system_keyspace)

        
class SnapSubCommand(subcommands.SubCommand):
    """Base for Sub Commands that watch and backup files. 
    """

    @classmethod
    def _common_args(cls):
        """Returns a :class:`argparser.ArgumentParser` with the common 
        arguments for this Command hierarchy.
        """

        common_args = super(SnapSubCommand, cls)._common_args()

        common_args.add_argument("-t", "--threads", type=int, default=4,
            help='Number of writer threads.')
        common_args.add_argument('-r', '--recursive', action='store_true', 
            default=False,
            help='Recursively watch the given path(s)s for new SSTables')
        common_args.add_argument('-a', '--auto-add', action='store_true', 
            dest='auto_add', default=False,
            help="Automatically start watching new subdirectories within "\
                "path(s)")
        common_args.add_argument('--exclude-keyspace', 
            dest='exclude_keyspace', nargs="*",
            help="User keyspaces to exclude from backup.")
        
        common_args.add_argument('--include-system-keyspace', default=False,
            dest='include_system_keyspace', action="store_true",
            help="Include the system keyspace.")

        common_args.add_argument('--test_mode', 
            action='store_true', dest='test_mode', default=False,
            help="Do not upload .")

        common_args.add_argument("cassandra_data_dir", 
            help="Top level Cassandra data directory, normally "\
            "/var/lib/cassandra/data.")

        return common_args

    def __init__(self, args):
        self.snap_config = SnapConfig.from_args(args)
        return

    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        # Make a queue, we put the files that need to be backed up here.
        file_q = Queue.Queue()

        # Make a watcher
        watcher = WatchdogWatcher(self.snap_config, file_q)

        # Make worker threads
        self.workers = [
            SnapWorkerThread(i, file_q, self._create_endpoint())
            for i in range(self.snap_config.threads)
        ]
        for worker in self.workers:
            worker.start()

        # Start the watcher
        watcher.start()

        self.log.info("Finished sub command %s" % self.command_name)
        return

    def _create_endpoint(self):
        """Called to create an endpoint to be used with a worker thread.

        Sublcasses must implement this.
        """
        raise NotImplementedError()

class SnapWorkerThread(threading.Thread):
    log = logging.getLogger("%s.%s" % (__name__, "SnapWorkerThread"))

    def __init__(self, thread_id, file_q, endpoint):
        super(SnapWorkerThread, self).__init__()

        self.name = "SnapWorker-%(thread_id)s" % vars()
        self.daemon = True

        self.file_q = file_q
        self.endpoint = endpoint

    def run(self):
        """Wait to get work from the :attr:`file_q`
        """

        while True:
            # blocking
            ks_manifest, cass_file = self.file_q.get()
            self.log.info("Uploading file %(cass_file)s" % vars())
            try:

                self.endpoint.store(ks_manifest, cass_file)
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

    def __init__(self, snap_config, file_q, refresh=True, 
        watch=True):
        
        self.snap_config = snap_config
        self.file_q = file_q
        self.refresh = refresh
        self.watch = watch

    def start(self):

        # initial read
        # always recursive for now
        if self.refresh:
            self.log.info("Refreshing existing files.")
            for root, dirs, files in os.walk(self.snap_config.cassandra_data_dir):
                for filename in files:
                    self._maybe_queue_file(os.path.join(root, filename))

        # watch if configured
        if not self.watch:
            return 

        observer = observers.Observer()
        observer.schedule(self, path=self.snap_config.cassandra_data_dir, 
            recursive=True)
        self.log.info("Watching for new file under %(cassandra_data_dir)s." %\
            vars(self.snap_config))

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

        if cass_file.descriptor.ks_name in self.snap_config.exclude_keyspaces:
            self.log.info("Ignoring file %s from excluded "\
                "keyspace %s" % (cass_file, cass_file.descriptor.ks_name))
            return False

        if (cass_file.descriptor.ks_name.lower() == "system") and (
            not self.snap_config.include_system_keyspace):

            self.log.info("Ignoring system keyspace file %(cass_file)s"\
                % vars())
            return False

        ks_manifest = file_util.KeyspaceManifest(
            self.snap_config.cassandra_data_dir, cass_file.descriptor.ks_name)
        self.log.info("Queueing file %(cass_file)s"\
            % vars())
        self.file_q.put((ks_manifest, cass_file))
        return True

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Watchdog file events.

    def on_created(self, event):
        self._maybe_queue_file(event.src_path)
        return

    def on_moved(self, event):
        self._maybe_queue_file(event.dest_path)
        return

