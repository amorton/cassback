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
# Purge - remove files from the backup

class PurgeSubCommand(subcommands.SubCommand):
    """
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
            help="Purge backups older than this date time.")
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
        
        
        endpoint = self._endpoint(self.args)
        
        purge_before = self._calc_purge_before()
        self.log.info("Purging backups older than: {purge_before}".format( 
            purge_before=purge_before))
        
        # Step 1- get all the manifests to be kept. 
        self.log.debug("Building list of manifests.")
        
        all_manifests = self._all_manifests(endpoint, self.args.keyspace, 
            self.args.host)
        self.log.info("Candiate manifest count: %s" % (len(all_manifests)))
        
        # Step 2 - work out which manifests are staying and which are being 
        # purged
        kept_manifests = []
        purged_manifests = []

        for manifest in all_manifests:
            if manifest.timestamp < purge_before:
                purged_manifests.append(manifest)
            else:
                kept_manifests.append(manifest)
        self.log.info("Will purge %s backups and keep %s" % (
            len(purged_manifests), len(kept_manifests)))

        # Step 3 - build a set of the files we want to keep. 
        # We do this even if there are no purged manifests. A we could be 
        # cleaning up after a failed purge.
        kept_files = set()
        for manifest in kept_manifests:
            kept_files.update(manifest.yield_file_names())
        self.log.info("Keeping %s sstable files." % (len(kept_files)))

        # Step 4 - Purge the manifests
        deleted_files = self._purge_manifests(endpoint, purged_manifests)

        # Step 5 - Purge the files that are not referenced from a manifest.
        deleted_files.extend(self._purge_sstables(endpoint, keyspaces, host, 
            kept_files))

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
        """Returns a list of backup days to read.
        
        These are the days we want to keep things from.
        """
        
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
        
        assert self.args.keep_days > 0
        return dt_util.now() - datetime.timedelta(self.args.keep_days)

    def _purge_manifests(self, endpoint,  purged_manifests):
        """Deletes the :cls:`cassandra.KeyspaceManifest` manifests 
        in ``purged_manifests``.
        
        Returns a list of the paths deleted. 
        """
        
        deleted = []
        for manifest in purged_manifests:
            path = manifest.backup_path
            self.log.info("Purging manifest file %(path)s" % vars())
            deleted.append(endpoint.remove_file(path, 
                dry_run=self.args.dry_run))
        return deleted

    def _purge_sstables(self, endpoint, keyspaces, host, kept_files):
        """Deletes the sstables for in the ``keyspaces`` for the ``host``
        that are not listed in ``kept_files``. 
        
        If ``keyspaces`` is empty purge from all keyspaces.
        
        Returns a list of the paths deleted.
        """
        
        # Step 1 - work out the keyspace directores we want to delete from.
        if keyspaces:
            ks_dirs = [
                os.path.join("hosts", host, ks_name)
                for ks_name in keyspaces
            ]
        else:
            host_dir = os.path.join("hosts", host)
            ks_dirs = list(
                os.path.join(host_dir, d)
                for d in endpoint.iter_dir(host_dir, include_files=False, 
                    include_dirs=True)
            )

        # Step 3 - delete files not in the keep list.
        deleted_files = []
        for ks_dir in ks_dirs:
            for file_path in endpoint.iter_dir(ks_dir, recursive=True):
                _, file_name = os.path.split(file_path)

                if file_name in kept_files:
                    self.log.debug("Keeping file %(file_path)s" % vars())
                else:
                    self.log.debug("Deleting file %(file_path)s" % vars())
                    deleted_files.append(endpoint.remove_file_with_meta(
                        file_path, dry_run=self.args.dry_run))

        return deleted_files
