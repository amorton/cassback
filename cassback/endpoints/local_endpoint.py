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

import errno
import json
import logging
import os.path
import shutil

from cassback import cassandra, file_util
from cassback.endpoints import endpoints

# ============================================================================ 
# Local endpoint, mostly for testing. 

class LocalEndpoint(endpoints.EndpointBase):
    
    log = logging.getLogger("%s.%s" % (__name__, "LocalEndpoint"))
    name = "local"
    
    _META_SUFFIX = "-meta.json"
    @classmethod
    def add_arg_group(cls, main_parser):
        """
        """
        
        group = main_parser.add_argument_group("local endpoint", 
            description="Configuration for the local endpoint.")

        group.add_argument('--backup-base', default=None, dest="backup_base",
            help="Base destination path.")

        return group

    def __init__(self, args):
        self.args = args


    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Endpoint Base Overrides 

    def backup_file(self, backup_file):
        
        dest_path = os.path.join(self.args.backup_base, 
            backup_file.backup_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        
        # Store the actual file
        with endpoints.TransferTiming(self.log, dest_path, 
            backup_file.component.stat.size):
            shutil.copy(backup_file.file_path, dest_path)
        
        # Store the meta data
        dest_meta_path = dest_path + self._META_SUFFIX
        with open(dest_meta_path, "w") as f:
            f.write(json.dumps(backup_file.serialise()))
        return dest_path

    def read_backup_file(self, path):
        
        dest_path = os.path.join(self.args.backup_base, path + "-meta.json")        
        with open(dest_path, "r") as f:
            return cassandra.BackupFile.deserialise(json.loads(f.read()))

    def backup_keyspace(self, ks_backup):

        dest_path = os.path.join(self.args.backup_base, ks_backup.backup_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        
        with open(dest_path, "w") as f:
            f.write(json.dumps(ks_backup.serialise()))
        return


    def restore_file(self, backup_file, dest_prefix):

        src_path = os.path.join(self.args.backup_base, 
            backup_file.backup_path)
        dest_path = os.path.join(dest_prefix, backup_file.restore_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        self.log.debug("Restoring file %s to %s", src_path, dest_path)
        
        with endpoints.TransferTiming(self.log, src_path, 
            backup_file.component.stat.size):
            shutil.copy(src_path, dest_path)
        return dest_path

    def exists(self, relative_path):
        path = os.path.join(self.args.backup_base, relative_path)
        return os.path.exists(path)

    def validate_checksum(self, relative_path, expected_md5_hex):
        path = os.path.join(self.args.backup_base, relative_path)

        current_md5 = file_util.file_md5(path)
        if current_md5 == expected_md5_hex:
            self.log.debug("Backup file %s matches expected md5 "\
                "%s", path, expected_md5_hex)
            return True
            
        self.log.warn("Backup file %s does not match expected md5 "\
            "%s, got %s", path, expected_md5_hex, current_md5)
        return False
        
    def read_keyspace(self, path):

        src_path = os.path.join(self.args.backup_base, path)
        with open(src_path, "r") as f:
            return cassandra.KeyspaceBackup.deserialise(json.loads(f.read()))

    def iter_dir(self, relative_path, include_files=True, 
        include_dirs=False, recursive=False):

        full_path = os.path.join(self.args.backup_base, relative_path)
        
        entries = []
        for root, dirs, files in os.walk(full_path):
            relative_root = root.replace(self.args.backup_base, "")
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
                        if not f.endswith(self._META_SUFFIX)
                    )
                else:
                    entries.extend(
                        f 
                        for f in files
                        if not f.startswith(self._META_SUFFIX)
                    )

            if not recursive:
                return entries
        return entries
        
    def remove_file(self, relative_path, dry_run=False):
        
        full_path = os.path.join(self.args.backup_base, relative_path)
        
        if dry_run:
            return full_path
            
        os.remove(full_path)
        file_util.maybe_remove_dirs(os.path.dirname(full_path))
        return full_path

    def remove_file_with_meta(self, relative_path, dry_run=False):
        
        # always try to delete meta data
        if not dry_run:
            meta_path = relative_path + self._META_SUFFIX
            try:
                self.remove_file(meta_path)
            except (EnvironmentError) as e:
                if not (e.errno == errno.ENOENT):
                    raise
        return self.remove_file(relative_path, dry_run=dry_run)

