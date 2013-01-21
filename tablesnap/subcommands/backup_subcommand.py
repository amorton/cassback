#!/usr/bin/env python
# encoding: utf-8

# Copyright 2012 Aaron Morton
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import copy
import datetime
import errno
import logging
import os
import os.path
import Queue
import socket
import time

from watchdog import events, observers

from tablesnap import cassandra, dt_util
from tablesnap.subcommands import subcommands

# ============================================================================
# Snap - used to backup files
        
class BackupSubCommand(subcommands.SubCommand):
    log = logging.getLogger("%s.%s" % (__name__, "BackupSubCommand"))

    command_name = "backup"
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

        sub_parser = super(BackupSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("--threads", type=int, default=4,
            help='Number of writer threads.')
        sub_parser.add_argument("--report-interval-secs", type=int, default=5,
            dest="report_interval_secs",
            help='Interval to report on the size of the work queue.')
            
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
            
        sub_parser.add_argument("--host",  
            default=socket.getfqdn(),
            help="Host to backup this node as.")

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
        
        if self.args.report_interval_secs > 0:
            reporter = SnapReporterThread(file_q, 
                self.args.report_interval_secs)
            reporter.start()
        else:
            self.log.info("Progress reporting is disabled.")
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

class SnapWorkerThread(subcommands.SubCommandWorkerThread):
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
            ks_backup, component = self.file_q.get()
            try:
                self._run_internal(endpoint, ks_backup, component)
            except (EnvironmentError) as e:
                # sometimes it's an IOError sometimes OSError
                # EnvironmentError is the base
                if not(e.errno == errno.ENOENT and \
                    e.filename==cass_file.file_path):
                    raise
                self.log.info("Aborted uploading %s as it was removed" %\
                    (component,))
            self.file_q.task_done()
        return

    def _run_internal(self, endpoint, ks_backup, component):
        """Backup the cassandra file and keyspace manifest. 

        Let errors from here buble out. 
        
        Returns `True` if the file was uploaded, `False` otherwise.
        """

        self.log.info("Uploading file %s" % (component,))
        
        # Create a BackupFile, this will have checksums 
        backup_file = cassandra.BackupFile(component.file_path, 
            host=self.args.host, component=component)

        # Store the cassandra file
        if endpoint.exists(backup_file.backup_path):
            if endpoint.validate_checksum(backup_file.backup_path, 
                backup_file.md5):
                
                self.log.info("Skipping file %s skipping as there is a "\
                    "valid backup"% (backup_file,))
            else:
                self.log.warn("Possibly corrupt file %s in the backup, "\
                    "skipping." % (backup_file,))
            return False
        
        uploaded_path = endpoint.store_with_meta(
            backup_file.component.file_path,
            backup_file.serialise(), backup_file.backup_path)
        endpoint.store_json(ks_backup.serialise(),
            ks_backup.backup_path)
        
        self.log.info("Uploaded file %s to %s" % (backup_file.file_path, 
            uploaded_path))
        return True

class SnapReporterThread(subcommands.SubCommandWorkerThread):
    """Watches the work queue and reports on progress. """
    log = logging.getLogger("%s.%s" % (__name__, "SnapReporterThread"))
    
    def __init__(self, file_q, interval):
        super(SnapReporterThread, self).__init__("SnapReporter-", 0)
        self.interval = interval
        self.file_q = file_q
    
    def _do_run(self):
        
        last_size = 0
        while True:
            
            size = self.file_q.qsize()
            if size > 0 or (size != last_size):
                self.log.info("Backup worker queue contains %s items "\
                    "(does not include tasks in progress)" % (size),)
            last_size = size
            time.sleep(self.interval)
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

        if cassandra.is_snapshot_path(file_path):
            self.log.info("Ignoring snapshot path %(file_path)s" % vars())
            return False

        try:
            component = cassandra.SSTableComponent(file_path)
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

        if component.temporary:
            self.log.info("Ignoring temporary file %(file_path)s" % vars())
            return False

        if component.keyspace in self.exclude_keyspaces:
            self.log.info("Ignoring file %s from excluded "\
                "keyspace %s" % (file_path, component.keyspace))
            return False

        if (component.keyspace.lower() == "system") and (
            not self.include_system_keyspace):

            self.log.info("Ignoring system keyspace file %(file_path)s"\
                % vars())
            return False

        ks_backup = cassandra.KeyspaceBackup(self.data_dir, 
            component.keyspace)
        self.log.info("Queueing file %(file_path)s"\
            % vars())
        self.file_queue.put((ks_backup, component))
        return True

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Watchdog file events.

    def on_created(self, event):
        self._maybe_queue_file(event.src_path)
        return

    def on_moved(self, event):
        self._maybe_queue_file(event.dest_path)
        return

