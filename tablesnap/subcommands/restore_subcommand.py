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
import datetime
import copy
import grp
import logging
import os.path
import pwd
import Queue
import json
import socket
import time

from tablesnap import cassandra, dt_util, file_util
from tablesnap.subcommands import subcommands

# ============================================================================
# Slurp - restore a backup

class RestoreSubCommand(subcommands.SubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "ListSubCommand"))

    command_name = "restore"
    command_help = "Restore backups"
    command_description = "Restore backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        
        sub_parser = super(RestoreSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("--threads", type=int, default=1,
            help='Number of writer threads.')
        sub_parser.add_argument("--report-interval-secs", type=int, default=5,
            dest="report_interval_secs",
            help='Interval to report on the size of the work queue.')
            
        sub_parser.add_argument("--cassandra_data_dir", 
            default="/var/lib/cassandra/data",
            help="Top level Cassandra data directory.")
        
        sub_parser.add_argument("--owner", 
            help="User to take ownership of restored files. Overrides "\
                "ownership included in the backup.")
        sub_parser.add_argument("--group", 
            help="Group to take ownership of restored files. Overrides "\
                "ownership included in the backup.")

        sub_parser.add_argument("--no-chown", default=False, 
            action="store_true", dest="no_chown",
            help="Do not chown files after restoring. Ignores owner, group "\
            "and the ownership included in the backup.")
        
        sub_parser.add_argument("--no-chmod", default=False, 
            action="store_true", dest="no_chmod",
            help="Do not chmod files after restoring. Ignores the "\
            "file mode included in the backup.")
            
        sub_parser.add_argument('backup_name',  
            help="Backup to restore.")

        return sub_parser

    def __init__(self, args):
        self._validate_args(args)
        self.args = args
        
    def _validate_args(self, args):
        
        if args.owner and not args.no_chown:
            try:
                pwd.getpwnam(args.owner)
            except (KeyError):
                raise argparse.ArgumentError(args.user, 
                    "Unknown user {user}".format(user=args.owner))
        
        if args.group and not args.no_chown:
            try:
                grp.getgrnam(args.group)
            except (KeyError):
                raise argparse.ArgumentError(args.group, 
                    "Unknown group {group}".format(group=group))
        return 
        
    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        endpoint = self._endpoint(self.args)
        manifest = self._load_manifest_by_name(endpoint, 
            self.args.backup_name)

        # We put the files that need to be restored in there.
        work_queue = Queue.Queue()
        # We put the results in here
        # So we can say what was copied to where.
        result_queue = Queue.Queue()

        # Fill the queue with components we want to restore.
        components = []
        for component in manifest.iter_components():
            work_queue.put(json.dumps(component.serialise()))
            components.append(component)
        self.log.info("Queued components for restore: %s", 
            ", ".join(str(c) for c in components))

        # Make worker threads to do the work. 
        workers = [
            self._create_worker_thread(i, work_queue, result_queue)
            for i in range(self.args.threads)
        ]
        for worker in workers:
            worker.start()
            
        if self.args.report_interval_secs > 0:
            reporter = SlurpReporterThread(work_queue, 
                self.args.report_interval_secs)
            reporter.start()
        else:
            self.log.info("Progress reporting is disabled.")

        # Wait for the work queue to empty. 
        self.log.info("Waiting on workers.")
        work_queue.join()
        self.log.info("Finished sub command %s" % self.command_name)

        # Make a pretty message 
        buffer = ["Restored files:"]
        restored_files = []
        # result_queue has BackupFile's
        while not result_queue.empty():
            restored_files.append(cassandra.RestoredFile.deserialise(
                json.loads(result_queue.get_nowait())))

        if restored_files:      
            buffer.extend(
                f.restore_msg()
                for f in restored_files
            )
        else:
            buffer.append("None")
        return (0, "\n".join(buffer))

    def _create_worker_thread(self, i, work_queue, result_queue):
        """Called to create an endpoint to be used with a worker thread.
        """
        return SlurpWorkerThread(i, work_queue, result_queue, 
            copy.copy(self.args))

class SlurpReporterThread(subcommands.SubCommandWorkerThread):
    """Watches the work queue and reports on progress. """
    log = logging.getLogger("%s.%s" % (__name__, "SlurpReporterThread"))
    
    def __init__(self, work_queue, interval):
        super(SlurpReporterThread, self).__init__("SlurpReporter-", 0)
        self.interval = interval
        self.work_queue = work_queue
    
    def _do_run(self):
        
        last_size = 0
        while True:
            
            size = self.work_queue.qsize()
            if size > 0 or (size != last_size):
                self.log.info("Slurp worker queue contains %s items "\
                    "(does not include tasks in progress)" % (size),)
            last_size = size
            time.sleep(self.interval)
        return
        
class SlurpWorkerThread(subcommands.SubCommandWorkerThread):
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
                return cassandra.SSTableComponent.deserialise(json.loads(
                    self.work_queue.get_nowait()))
            except (Queue.Empty):
                return None

        endpoint = self._endpoint(self.args)
        component = safe_get()
        while component is not None:
            self.log.info("Restoring component %s under %s", component, 
                self.args.cassandra_data_dir)
            
            # We need a backup file for the component, so we know 
            # where it is stored and where it will backup to 
            # we also want the MD5, that is on disk
            backup_file = endpoint.read_backup_file(cassandra.BackupFile(
                None, component=component, md5="").backup_path)
                
            # Restore the file if we want to
            should_restore, reason = self._should_restore(backup_file, 
                self.args.cassandra_data_dir)
            if should_restore:
                
                restore_path = endpoint.restore_file(backup_file, 
                    self.args.cassandra_data_dir)
                self.log.info("Restored file %s to %s", backup_file, 
                    restore_path)
                
                if not self.args.no_chown:
                    self._chown_restored_file(backup_file, restore_path, 
                        self.args.owner, self.args.group)
                if not self.args.no_chmod:
                    self._chmod_restored_file(backup_file, restore_path)
                
                self.result_queue.put(json.dumps(cassandra.RestoredFile(True, 
                    restore_path, backup_file).serialise()))
            
            else:
                self.log.info("Skipping file %s because %s", backup_file, 
                    reason)
                self.result_queue.put(json.dumps(cassandra.RestoredFile(
                    False, None, backup_file, reason_skipped=reason
                    ).serialise()))

            self.work_queue.task_done()
            component = safe_get()
        return

    def _should_restore(self, backup_file, dest_prefix):
        """Called to test if the ``backup_file`` should be restored to 
        ``dest_path``.

        Returns a tuple of ``(should_restore, reason)`` where ``reason`` 
        is a string description to say why not, .e.g "Existing file"
        """

        if os.path.exists(os.path.join(dest_prefix, 
            backup_file.restore_path)):
            return (False, "Existing file")

        return (True, None)
    
    def _chown_restored_file(self, backup_file, restore_path, arg_user, 
        arg_grp):
        """Restore ownership of the file at ``restore_path`` to either the 
        user and group in ``backup_file`` or the ``arg_user`` and ``arg_grp`` 
        if specified.
        """
        
        user = arg_user or backup_file.component.stat.user
        if not user:
            self.log.warn("Could not determine user name to chown %s", 
                restore_path)
            uid = -1
        else:
            # will raise a KeyError on error let it fail.
            uid = pwd.getpwnam(user).pw_uid
        
        group = arg_grp or backup_file.component.stat.group
        if not group:
            self.log.warn("Could not determine group name to chown %s", 
                restore_path)
            gid = -1
        else:
            # will raise a KeyError on error let it fail.
            gid = grp.getgrnam(group).gr_gid
        
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("chown'ing {restore_path} to {user}/{uid} and "\
                "{group}/{gid}".format(**vars()))
        os.chown(restore_path, uid, gid)
        return

    def _chmod_restored_file(self, backup_file, restore_path):
        """Restore mode of the ``backup_file`` restored to ``restore_path``.
        """
        
        if not backup_file.component.stat.mode:
            self.log.warn("Could not determine file mode for restored file "\
                "%s at %s", backup_file, restore_path)
            return

        self.log.debug("chmoding %s to %s", restore_path, 
            backup_file.component.stat.mode)
        os.chmod(restore_path, backup_file.component.stat.mode)
        return
