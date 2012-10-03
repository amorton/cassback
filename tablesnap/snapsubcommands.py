import os
import Queue
import threading

from watchdog import events

import tablesnap

class SnapConfig(object):

    def __init__(self, watch_path, threads, recursive, auto_add):
        self.watch_path = watch_path
        self.threads = threads
        self.recursive = recursive
        self.auto_add = auto_add

    @classmethod
    def from_args(cls, args):
        return SnapConfig(args.watch_path, args.threads, args.recursive,
            args.auto_add)

        
class SnapSubCommand(tablesnap.SubCommand):
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

        common_args.add_argument('watch-path', 
            help='Path to watch.')

        return common_args

    def __init__(self, args):
        self.snap_config = SnapConfig.from_args(args)
        return

    def __call__(self):

        # Make a queue
        file_q = Queue.Queue()

        # Make a watcher
        watcher = WatchdogWatcher(self.snap_config.watch_path, 
            file_q, file_util.is_not_temp)

        # Make worker threads
        self.workers = [
            SnapWorkerThread(i, self._create_uploader())
            for i in enumerate(self.snap_config.threads)
        ]
        for worker in self.workers:
            worker.start()

        # Start the watcher
        watcher.start()
        return

    def _create_uploader(self):
        """Called to create an uploader to be used with a worker thread."""
        raise NotImplementedException()

class SnapWorkerThread(threading.Thread):

    def __init__(self, thread_id, file_q, uploader):
        
        self.name = "SnapWorker-%(thread_id)s" % vars()
        self.daemon = True

        self.file_q = file_q
        self.uploader = uploader

    def run(self):
        """Wait to get work from the :attr:`file_q`
        """

        while True:
            # blocking
            file_path = self.fileq.get()

            try:
                self.uploader.upload(file_path)
            except (Exception):
                # self.log.critical("Failed uploading %s. Aborting.\n%s" %
                #              (file_path, format_exc()))
                # Brute force kill self
                os.kill(os.getpid(), signal.SIGKILL)

            self.fileq.task_done()
        return



class WatchdogWatcher(events.FileSystemEventHandler):

    def __init__(self, watch_path, file_q, file_filter, restart=True, watch=True):
        self.watch_path = watch_path
        slelf.file_filter = file_filter
        self.file_q = file_q
        self.restart = restart
        self.watch = watch

    def start(self):

        # initial read

        # watch if configured

        return

    def on_moved(self, event):

        pass




