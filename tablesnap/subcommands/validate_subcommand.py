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
import logging

from tablesnap import cassandra
from tablesnap.subcommands import subcommands

# ============================================================================
# Validate - validate that all files in a backup are present

class ValidateSubCommand(subcommands.SubCommand):
    """Base for Sub Commands that watch and backup files. 
    """

    log = logging.getLogger("%s.%s" % (__name__, "ListSubCommand"))

    command_name = "validate"
    command_help = "Validate all files exist for a backup."
    command_description = "Validate all files exist for a backup."

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        sub_parser = super(ValidateSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("--checksum",
            action='store_true', dest='checksum', default=False,
            help="Do an MD5 checksum of the files.")

        sub_parser.add_argument('backup_name',  
            help="Backup to validate.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):

        self.log.info("Starting sub command %s" % self.command_name)

        endpoint = self._endpoint(self.args)
        manifest = self._load_manifest(endpoint, self.args.backup_name)

        missing_files = []
        present_files = []
        corrupt_files = []

        for file_name in manifest.yield_file_names():
            
            # Model the file in the manifest.
            cass_file = cassandra.CassandraFile.from_file_path(file_name, 
                meta={}, host=manifest.host)

            try:
                cass_file.meta = endpoint.read_meta(cass_file.backup_path)
            except (EnvironmentError) as e:
                if e.errno != errno.ENOENT:
                    raise
                # missing file is ok. 
                cass_file = None    

            if cass_file is None:
                missing_files.append(file_name)
            elif endpoint.exists(cass_file.backup_path):
                if not self.args.checksum:
                    present_files.append(file_name)
                elif endpoint.validate_checksum(cass_file.backup_path, 
                    cass_file.meta["md5_hex"]):
                    present_files.append(file_name)
                else:
                    corrupt_files.append(file_name)
            else:
                missing_files.append(file_name)

        buffer = []
        if missing_files or corrupt_files:
            buffer.append("Missing or corrupt files found for backup "\
                "%s" % (self.args.backup_name,))
        else:
            buffer.append("All files present for backup %s"\
                 % (self.args.backup_name,))

        if self.args.checksum:
            buffer.append("Files were checksummed")
        else:
            buffer.append("Files were not checksummed")

        buffer.append("")

        if corrupt_files:
            buffer.append("Corrupt Files:")
            buffer.extend(corrupt_files)

        if missing_files:
            buffer.append("Missing Files:")
            buffer.extend(missing_files)

        if present_files:
            buffer.append("Files:")
            buffer.extend(present_files)

        self.log.info("Finished sub command %s" % self.command_name)
        return (
            0 if not missing_files and not corrupt_files else 1,
            "\n".join(buffer)
        )
