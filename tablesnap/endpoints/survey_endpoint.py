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

import json
import logging
import os.path

from tablesnap import cassandra, dt_util, file_util
from tablesnap.endpoints import endpoints


# ============================================================================ 
# Survey endpoint writes all operations to a file without doing anything 
# Used to estimate S3 billing etc. 

class SurveyEndpoint(endpoints.EndpointBase):
    
    log = logging.getLogger("%s.%s" % (__name__, "SurveyEndpoint"))
    name = "survey"
    
    FILE_OP_PATTERN = "{time} {action} {src_path} to {dest_path} "\
        "size {bytes}\n"
    
    def __init__(self, args):
        self.args = args
        
        self.log_file = open(self.args.survey_path, "a+")
        
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Endpoint Base Overrides

    @classmethod
    def add_arg_group(cls, main_parser):
        """
        """
        
        group = main_parser.add_argument_group("survey endpoint", 
            description="Configuration for the survey endpoint.")

        group.add_argument('--survey-path', dest="survey_path",
            help="Path to the survey log file.")
        group.add_argument('--meta-dir', dest="meta_dir",
            help="Directory to store meta data files in.")
        return group

    @classmethod
    def validate_args(cls, args):
        
        if os.path.isfile(args.survey_path):
            return
        
        if os.path.exists(args.survey_path):
            raise argparse.ArgumentError("--survey-path",
                "Path exists and is not a file.")
        
        file_util.ensure_dir(os.path.dirname(args.survey_path))
        file_util.ensure_dir(args.meta_dir)
        return
        
    def backup_file(self, backup_file):
        """Writes the meta data and upates the survey log."""
        
        msg = self.FILE_OP_PATTERN.format(time=dt_util.now_iso(), 
            action="stored", src_path=backup_file.file_path, 
            dest_path=backup_file.backup_path, 
            bytes=backup_file.component.stat.size)
        self.log_file.write(msg)
        self.log_file.flush()
        
        dest_path = os.path.join(self.args.meta_dir, backup_file.backup_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        with open(dest_path, "w") as f:
            f.write(json.dumps(backup_file.serialise()))
        return relative_dest_path

    def read_backup_file(self, path):

        path = os.path.join(self.args.meta_dir, path)
        with open(path, "r") as f:
            return cassandra.BackupFile.deserialise(json.loads(f.read()))

    def backup_keyspace(self, ks_backup):
        
        dest_path = os.path.join(self.args.meta_dir, ks_backup.backup_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        with open(dest_path, "w") as f:
            f.write(json.dumps(ks_backup.serialise()))
        return


    def restore_file(self, backup_file, dest_prefix):
        
        dest_path = os.path.join(dest_prefix, backup_file.restore_path)
        msg = self.FILE_OP_PATTERN.format(time=dt_util.now_iso(), 
            action="restored", src_path=backup_file, 
            dest_path=dest_path, 
            bytes=backup_file.component.stat.size)
        self.log_file.write(msg)
        self.log_file.flush()
        return

    def exists(self, relative_path):
        path = os.path.join(self.args.meta_dir, relative_path)
        return os.path.exists(path)

    def validate_checksum(self, relative_path, expected_md5_hex):
        return True

    def read_keyspace(self, path):

        src_path = os.path.join(self.args.meta_dir, path)
        with open(src_path, "r") as f:
            return cassandra.KeyspaceBackup.deserialise(json.loads(f.read()))

    def iter_dir(self, relative_path, include_files=True, 
        include_dirs=False, recursive=False):

        full_path = os.path.join(self.args.meta_dir, relative_path)
        
        entries = []
        for root, dirs, files in os.walk(full_path):
            relative_root = root.replace(self.args.meta_dir, "")
            if relative_root.startswith("/"):
                relative_root = relative_root[1:]
            
            if include_dirs:
                if recursive:
                    entries.extend(
                        os.path.join(relative_root, d)
                        for d in dirs
                    )
                else:
                    entries.extend(dirs)
                    
            if include_files:
                if recursive:
                    entries.extend(
                        os.path.join(relative_root, f)
                        for f in files
                    )
                else:
                    entries.extend(
                        f 
                        for f in files
                    )

            if not recursive:
                return entries
        return entries
        
    def remove_file(self, relative_path, dry_run=False):
        
        path = os.path.join(self.args.meta_dir, relative_path)
        restore_file = self.read_backup_file(relative_path)
        
        if dry_run:
            return path
            
        # If no size do not add to the survey, 
        # it's probably not a file with meta data.
        # e.g. we are deleting a manifest
        if size is not None:
            msg = self.FILE_OP_PATTERN.format(time=dt_util.now_iso(), 
                action="removed", src_path=relative_path, 
                dest_path="na", 
                bytes=restore_file.component.stat.size)
            
            self.log_file.write(msg)
            self.log_file.flush()
        
        os.remove(path)
        file_util.maybe_remove_dirs(os.path.dirname(path))
        return relative_path

    def remove_file_with_meta(self, relative_path, dry_run=False):
        
        return self.remove_file(relative_path, dry_run=dry_run)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # 
    