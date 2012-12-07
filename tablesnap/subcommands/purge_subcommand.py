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

        sub_parser.add_argument("--keyspace", nargs="+",
            help="Keyspace to purge from, if not specified all keyspaces "\
            "are purged.")

        sub_parser.add_argument("--host",  
            help="Host to purge backups from, defaults to this host.")

        sub_parser.add_argument("--purge-before",
            dest='purge_before',
            help="Purge backups older than this date time.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):
        
        
        keyspaces = self.args.keyspace
        host = self.args.host or socket.getfqdn()
        purge_before = dt_util.parse_date_input(self.args.purge_before)
        
        endpoint = self._endpoint(self.args)
        
        # Step 1- get all the manifests
        self.log.debug("Building list of manifests.")
        all_manifests = self._all_manifests(endpoint, keyspaces, host)
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

        buffer = ["Deleted files:", ""]
        if deleted_files:
            buffer.extend(deleted_files)
        else:
            buffer.append("No files")

        return (0, "\n".join(buffer))

    def _all_manifests(self, endpoint, keyspaces, host):
        """Loads all manifests for the ``keyspaces`` and ``host``.
        """
        
        # Step 1 - get the keyspace dirs
        # if keyspace arg is empty then get all. 
        if keyspaces:
            ks_dirs = [
                os.path.join("cluster", ks_name)
                for ks_name in keyspaces
            ]
        else:
            ks_dirs = list(
                os.path.join("cluster", d)
                for d in endpoint.iter_dir("cluster", include_files=False, 
                    include_dirs=True)
            )
            
        # Step 2 - Load the manifests
        manifests = []
        for ks_dir in ks_dirs:
            for manifest_file_name in endpoint.iter_dir(ks_dir):
                # Just load the manifest and check the host
                # could be better. 
                self.log.debug("Opening manifest file %(manifest_file_name)s"\
                    % vars())

                manifest = cassandra.KeyspaceManifest.from_manifest(
                    endpoint.read_json(os.path.join(ks_dir, 
                    manifest_file_name)))

                if manifest.host == host:
                    manifests.append(manifest)
        return manifests

    def _purge_manifests(self, endpoint,  purged_manifests):
        """Deletes the :cls:`cassandra.KeyspaceManifest` manifests 
        in ``purged_manifests``.
        
        Returns a list of the paths deleted. 
        """
        
        deleted = []
        for manifest in purged_manifests:
            path = manifest.backup_path
            self.log.info("Purging manifest file %(path)s" % vars())
            deleted.append(endpoint.remove_file(path))
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
                        file_path))

        return deleted_files
