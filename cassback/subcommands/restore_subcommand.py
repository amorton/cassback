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
import grp
import itertools
import logging
import os.path
import pwd
import Queue
import json
import shutil
import signal
import socket
import time

from cassback import cassandra, dt_util, file_util
from cassback.subcommands import subcommands

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
            
        sub_parser.add_argument("--data-dir", dest="data_dir",
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

        sub_parser.add_argument("--no-checkum", default=False, 
            action="store_true", dest="no_checksum",
            help="If specified and a target file exists restoring the file "\
                "is skipped without checkum. Additionally when a file is "\
                "restored is it not checksumed.")
        sub_parser.add_argument("--fail-if-corrupt", default=False, 
            action="store_true", dest="fail_if_corrupt",
            help="If a restored file is corrupt fail processing the "\
                "entire backup.")

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
                    "Unknown user %s" % (user,))
        
        if args.group and not args.no_chown:
            try:
                grp.getgrnam(args.group)
            except (KeyError):
                raise argparse.ArgumentError(args.group, 
                    "Unknown group %s" % (group,))
        return 
        
    def __call__(self):

        self.log.info("Starting sub command %s", self.command_name)

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
            msg = (
                json.dumps(manifest.serialise()),
                json.dumps(component.serialise())
            )
            work_queue.put(msg)
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
        self.log.info("Finished sub command %s", self.command_name)

        # Make a pretty message 
        op_results = []
        # result_queue has BackupFile's
        while not result_queue.empty():
            op_results.append(RestoreResult.deserialise(json.loads(
                result_queue.get_nowait())))
        
        str_bld = []
        failed_ops = [
            op_result
            for op_result in op_results
            if op_result.failed_reason
        ]
        
        skipped_ops = [
            op_result
            for op_result in op_results
            if not op_result.should_restore
        ]
        success_ops = [
            op_result
            for op_result in op_results
            if not op_result.failed_reason and op_result.should_restore
        ]
        corrupt_files = [
            op_result
            for op_result in op_results
            if op_result.corrupt_path
        ]
        
        def gen_groups(lst, key):
            lst.sort(key=key)
            for k, i in itertools.groupby(lst, key):
                yield k, i
                
        if corrupt_files:
            str_bld.append("Moved corrupt existing files:")
            for op_result in corrupt_files:
                str_bld.append("%s -> %s" % ( 
                    op_result.restore_path, op_result.corrupt_path))
            str_bld.append("")
            
        if failed_ops:
            for k, ops in gen_groups(failed_ops, lambda x: x.failed_reason):
                
                str_bld.append("Failed transfers - %s:" % (k,))
                for op_result in ops:
                    _, name = os.path.split(op_result.source_path)
                    str_bld.append("%s -> %s \tbecause %s" % (
                        name, op_result.restore_path, op_result.restore_reason))
                str_bld.append("")

        if skipped_ops:
            str_bld.append("Skipped files:")
            for op_result in skipped_ops:
                _, name = os.path.split(op_result.source_path)
                str_bld.append("%s -> %s \tbecause %s" %( 
                    name, op_result.restore_path, op_result.restore_reason))
            str_bld.append("")
                    
        if success_ops:
            str_bld.append("Restored files:")
            for op_result in success_ops:
                _, name = os.path.split(op_result.source_path)
                str_bld.append("%s -> %s \tbecause %s" %( 
                    name, op_result.restore_path, op_result.restore_reason))
            str_bld.append("")
        
        return (0, "\n".join(str_bld))

    def _create_worker_thread(self, i, work_queue, result_queue):
        """Called to create an endpoint to be used with a worker thread.
        """
        return SlurpWorkerThread(i, work_queue, result_queue, 
            copy.copy(self.args))

# ============================================================================
# Reporter thread for watching the queue size

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

# ============================================================================
# Worker thread for restoring files

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
                ks_backup_json, component_json = self.work_queue.get_nowait()
                
                return (
                    cassandra.KeyspaceBackup.deserialise(
                        json.loads(ks_backup_json)),
                    cassandra.SSTableComponent.deserialise(
                        json.loads(component_json))
                )
            except (Queue.Empty):
                return (None, None)

        endpoint = self._endpoint(self.args)
        manifest, component = safe_get()
        while component is not None:
            self.log.info("Restoring component %s under %s", component, 
                self.args.data_dir)
            
            # We need a backup file for the component, so we know 
            # where it is stored and where it will backup to 
            # we also want the MD5, that is on disk
            backup_file = endpoint.read_backup_file(cassandra.BackupFile(
                None, component=component, md5="", 
                host=manifest.host).backup_path)
            
            operation = RestoreOperation(endpoint, backup_file, 
                copy.copy(self.args))
            op_result = operation()
            
            self.work_queue.task_done()
            self.result_queue.put(json.dumps(op_result.serialise()))
            manifest, component = safe_get()
        return



# ============================================================================
# Result of a restore operation

class RestoreOperation(object):
    log = logging.getLogger("%s.%s" % (__name__, "RestoreOperation"))
    
    def __init__(self, endpoint, backup_file, args):
        self.endpoint = endpoint
        self.args = args
        self.backup_file = backup_file

    def __call__(self):
        """Run the restore operation and store the result."""
                
        op_result = self._should_restore()
        if not op_result.should_restore:
            self.log.info("Skipping file %s because %s", self.backup_file, 
                op_result.restore_reason)
            return op_result
            
        self.log.info("Restoring file %s because %s", self.backup_file, 
            op_result.restore_reason)
        
        try:
            restore_path = self.endpoint.restore_file(self.backup_file, 
                self.args.data_dir)
        except (EnvironmentError) as e:
            if e.errno != errno.ENOENT:
                raise
            # source file was missing
            op_result.failed_reason = "Source file missing" 
            return op_result
            
        # Until I change the endpoint lets check this
        assert restore_path == op_result.restore_path
        self.log.info("Restored file %s to %s", self.backup_file, 
            op_result.restore_path)

        if not self.args.no_chown:
            self._chown_restored_file(op_result)
        if not self.args.no_chmod:
            self._chmod_restored_file(op_result)
        if self.args.no_checksum:
            self.log.debug("Restored file at %s was not checksumed", 
                op_result.restore_path)
            return op_result

        existing_md5 = file_util.file_md5(op_result.restore_path)
        if existing_md5 != self.backup_file.md5:
            # Better handling ? 
            self.log.error("Restored file %s has MD5 %s, expected %s",
                op_result.restore_path, existing_md5, 
                self.backup_file.md5)

            self.log.info("Deleting corrupt restored file %s", 
                op_result.restore_path)
            try:
                os.remove(op_result.restore_path)
            except (Exception):
                self.log.error("Error deleting corrupt restored file %s", 
                    op_result.restore_path, exc_info=True)
                print "Error deleting corrupt restored file %s" % (
                    op_result.restore_path)
                os.kill(os.getpid(), signal.SIGKILL)
                
            if self.args.fail_if_corrupt:
                # This is probably the wrong thing to do
                print "Restored file %s has MD5 %s, expected %s" % (
                op_result.restore_path, existing_md5, self.backup_file.md5)
                print "Killing self."
                
                os.kill(os.getpid(), signal.SIGKILL)
            op_result.failed_reason = "Restored File corrupt"
        return op_result
            
    def _should_restore(self):
        """Determines if the :attr:`backup_file` should be restored.
        """
        
        dest_path = os.path.join(self.args.data_dir, 
            self.backup_file.restore_path)
        
        if not os.path.exists(dest_path):
            # no file, lets restore
            return RestoreResult(should_restore=True, 
                restore_reason="No File", restore_path=dest_path, 
                source_path=self.backup_file.backup_path)
            
        if self.args.no_checksum:
            # exsisting file and no checksum, do not restore
            return RestoreResult(should_restore=True, 
                restore_reason="Existing file (Not checksummed)", 
                restore_path=dest_path, 
                source_path=self.backup_file.backup_path)
        
        existing_md5 = file_util.file_md5(dest_path)
        if existing_md5 == self.backup_file.md5:
            return RestoreResult(should_restore=False, 
                restore_reason="Existing file (Checksummed)", 
                restore_path=dest_path, 
                source_path=self.backup_file.backup_path)

        # move the current file to 
        # $data_dir/../cassback-corrupt/keyspace/$file_name
        _, file_name = os.path.split(dest_path)
        corrupt_path = os.path.join(*(self.args.data_dir, "..",
            "cassback-corrupt", self.backup_file.component.keyspace, 
            file_name))
        file_util.ensure_dir(os.path.dirname(corrupt_path))
        self.log.info("Moving existing corrupt file %s to %s", dest_path, 
            corrupt_path)
        shutil.move(dest_path, corrupt_path)
        
        return RestoreResult(should_restore=True, 
            restore_reason="Existing file corrupt", restore_path=dest_path, 
            corrupt_path=corrupt_path, 
            source_path=self.backup_file.backup_path)

    def _chown_restored_file(self, op_result):

        user = self.args.owner or self.backup_file.component.stat.user
        if not user:
            self.log.warn("Could not determine user name to chown %s", 
                op_result.restore_path)
            uid = -1
        else:
            # will raise a KeyError on error let it fail.
            uid = pwd.getpwnam(user).pw_uid
        
        group = self.args.group or self.backup_file.component.stat.group
        if not group:
            self.log.warn("Could not determine group name to chown %s", 
                op_result.restore_path)
            gid = -1
        else:
            # will raise a KeyError on error let it fail.
            gid = grp.getgrnam(group).gr_gid
        
        self.log.debug("chown'ing %s to %s/%s and %s/%s", 
            op_result.restore_path, user, uid, group, gid)
        os.chown(op_result.restore_path, uid, gid)
        return

    def _chmod_restored_file(self, op_result):
        """
        """
        
        if not self.backup_file.component.stat.mode:
            self.log.warn("Could not determine file mode for restored file "\
                "%s at %s", self.backup_file, op_result.restore_path)
            return

        self.log.debug("chmoding %s to %s", op_result.restore_path, 
            self.backup_file.component.stat.mode)
        os.chmod(op_result.restore_path, self.backup_file.component.stat.mode)
        return

# ============================================================================
# Result of a restore operation

class RestoreResult(object):
    log = logging.getLogger("%s.%s" % (__name__, "RestoreResult"))
    
    def __init__(self, should_restore=True, failed_reason=None, 
        restore_reason=None, source_path=None, restore_path=None, 
        corrupt_path=False):
        
        self.should_restore = should_restore
        self.failed_reason = failed_reason
        self.restore_reason = restore_reason
        
        self.source_path = source_path
        self.restore_path = restore_path
        self.corrupt_path = corrupt_path
    
    def serialise(self):
        return dict(vars(self))
    
    @classmethod
    def deserialise(cls, data):
        return cls(**data)
