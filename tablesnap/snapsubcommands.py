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

    def __init__(self, watch_path, threads, recursive, auto_add, skip_index,
        test_mode):
        self.watch_path = watch_path
        self.threads = threads
        self.recursive = recursive
        self.auto_add = auto_add
        self.skip_index = skip_index
        self.test_mode = test_mode

    @classmethod
    def from_args(cls, args):
        return SnapConfig(args.watch_path, args.threads, args.recursive,
            args.auto_add, args.skip_index, args.test_mode)

        
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
        common_args.add_argument('--skip-index', 
            action='store_true', dest='skip_index', default=False,
            help="Disable storing a JSON index of all files in the snapshot.")

        common_args.add_argument('--test_mode', 
            action='store_true', dest='test_mode', default=False,
            help="Do not upload .")

        common_args.add_argument('watch_path', 
            help='Path to watch.')

        return common_args

    def __init__(self, args):
        self.snap_config = SnapConfig.from_args(args)
        return

    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        # Make a queue, we put the files that need to be backed up here.
        file_q = Queue.Queue()

        # Make a watcher
        watcher = WatchdogWatcher(self.snap_config.watch_path, file_q)

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
            cass_file = self.file_q.get()
            self.log.info("Uploading file %(cass_file)s" % vars())
            try:

                self.endpoint.store(cass_file)
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

    def __init__(self, watch_path, file_q, refresh=True, 
        watch=True):
        
        self.watch_path = watch_path
        self.file_q = file_q
        self.refresh = refresh
        self.watch = watch

    def start(self):

        # initial read
        # always recursive for now
        if self.refresh:
            for root, dirs, files in os.walk(self.watch_path):
                for filename in files:
                    self._maybe_queue_file(os.path.join(root, filename))

        # watch if configured
        if not self.watch:
            return 

        observer = observers.Observer()
        observer.schedule(self, path=self.watch_path, recursive=True)
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

        if cass_file.should_backup():
            self.log.info("Queued file %(file_path)s" % vars())
            self.file_q.put(cass_file)
            return True

        self.log.info("Ignoring file %(file_path)s" % vars())
        return False

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Watchdog file events.

    def on_created(self, event):
        self._maybe_queue_file(event.src_path)
        return

    def on_moved(self, event):
        self._maybe_queue_file(event.dest_path)
        return

