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
import logging
import os.path
import socket

from tablesnap import cassandra, dt_util
from tablesnap.subcommands import subcommands

# ============================================================================
# 

class PurgeSubCommand(subcommands.SubCommand):
    """Purge command to remove files from the backup
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

        sub_parser.add_argument("--host",  
            default=socket.getfqdn(),
            help="Host to purge backups from, defaults to this host.")
        sub_parser.add_argument("--dry-run", dest="dry_run", default=False,
            action="store_true",
            help="Do not delete any files.")
            
        purge_group = sub_parser.add_mutually_exclusive_group(required=True)
        purge_group.add_argument("--purge-before",
            dest='purge_before',
            help="Purge backups older than this ISO date time.")
        purge_group.add_argument("--keep-days",
            dest='keep_days', type=int, 
            help="Number of days of backups to keep.")

        sub_parser.add_argument("keyspace",
            help="Keyspace to purge from.")
            
        return sub_parser

    def __init__(self, args):
        self.args = args
        self._validate_args()
        
    def _validate_args(self):
        
        if self.args.keep_days is not None and self.args.keep_days < 1:
            raise argparse.ArgumentError(None, "keep_days must "\
                "be greater than 0. ")
        return
        
    def __call__(self):
        """Implements the command."""
        
        endpoint = self._endpoint(self.args)
        
        # Work out which days we want to keep 
        # the exact cut off time for backups to keep
        purge_before = self._calc_purge_before()
        # list of full or partial days to keep
        manifest_days = self._manifest_days(purge_before)
        self.log.info("To purge backups older then %s will read "\
            " manifests from %s", purge_before, manifest_days)
        
        # Read the manifests we want to keep. 
        self.log.debug("Reading manifests after %s to keep.", purge_before)
        manifests = []
        for manifest_day in manifest_days:
            for manifest in self._list_manifests(endpoint, 
                self.args.keyspace, self.args.host, manifest_day):
                
                if manifest.timestamp >= purge_before:
                    manifests.append(manifest)
                else:
                    self.log.debug("Will not keep manifest %s", manifest)
        self.log.info("Keeping backups %s", manifests)
        

        # Build a list of the files we want to keep. 
        # We purge everything else so that a failed purge can be fixed.
        keep_components = []
        for manifest in manifests:
            keep_components.extend(manifest.iter_components())
        self.log.info("Keeping sstable files: %s", keep_components)

        # Step 4 - Purge the manifests
        deleted_files = self._purge_manifests(endpoint, manifests)

        # Step 5 - Purge the files that are not referenced from a manifest.
        deleted_files.extend(self._purge_sstables(endpoint, keep_components))

        str_build = ["Purge backups before {purge_before}".format(
            purge_before=dt_util.to_iso(purge_before))
        ]
        if self.args.dry_run:
            str_build.append("DRY RUN: no files deleted, candidate files:")
        else:
            str_build.append("Deleted files:")
        str_build.append("")
        
        if deleted_files:
            str_build.extend(deleted_files)
        else:
            str_build.append("No files")

        return (0, "\n".join(str_build))
        
    def _manifest_days(self, purge_before):
        """Returns a list of backup days we want to read the manifest for. 
        These are the days we potentially want to keep.
        
        """
        
        # Normalise the purge_before to be a whole day. 
        from_day = datetime.datetime(purge_before.year, purge_before.month, 
            purge_before.day)
        now = dt_util.now()
        to_day = datetime.datetime(now.year, now.month, now.day)
        
        assert from_day <= to_day
        diff = to_day - from_day
        
        return [
            from_day + datetime.timedelta(d)
            for d in range(diff.days + 1)
        ]
        
    def _calc_purge_before(self):
        """Calculates time after which manifests should be purged."""
        
        if self.args.purge_before:
            return dt_util.parse_date_input(self.args.purge_before)
        
        assert (self.args.keep_days or -1) > 0
        return dt_util.now() - datetime.timedelta(self.args.keep_days)

    def _purge_manifests(self, endpoint,  keep_manifests):
        """Deletes all manifests for the current keyspace and host not in 
        the list of ``kept_manifests``
        
        Returns a list of the paths deleted. 
        """
        
        keep_paths = frozenset(
            manifest.backup_path
            for manifest in keep_manifests
        )
        self.log.debug("Keeping manifsest paths {keep_paths}".format(
            keep_paths=keep_paths))
                    
        keyspace_dir = cassandra.KeyspaceBackup.backup_keyspace_dir(
            self.args.keyspace)
        deleted = []
        for manifest_path in endpoint.iter_dir(keyspace_dir, recursive=True):
            if manifest_path in keep_paths:
                self.log.debug("Keeping manifest %s", manifest_path) 
            else:
                self.log.debug("Purging manifest %s", manifest_path)
                deleted.append(endpoint.remove_file(manifest_path, 
                    dry_run=self.args.dry_run))
        return deleted

    def _purge_sstables(self, endpoint, kept_components):
        """Deletes the sstables for the current keyspace and host
        that are not listed in ``kept_components``. 
        
        Returns a list of the paths deleted.
        """
        
        ks_dir = cassandra.BackupFile.backup_keyspace_dir(self.args.host, 
            self.args.keyspace)
        
        set_kept_files = frozenset(
            component.backup_file_name
            for component in kept_components
        )
        
        # Step 3 - delete files not in the keep list.
        deleted_files = []
        for file_path in endpoint.iter_dir(ks_dir, recursive=True):
            _, file_name = os.path.split(file_path)

            if file_name in set_kept_files:
                self.log.debug("Keeping file %s", file_path)
            else:
                self.log.debug("Deleting file %s", file_path)
                deleted_files.append(endpoint.remove_file_with_meta(
                    file_path, dry_run=self.args.dry_run))
        return deleted_files
