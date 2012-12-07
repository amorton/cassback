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


"""Base for commands"""

import logging
import os.path

from tablesnap import dt_util
from tablesnap.subcommands import subcommands

# ============================================================================
# Show - show the contents of a backup

class ShowSubCommand(subcommands.SubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "ShowSubCommand"))

    command_name = "show"
    command_help = "Show the contents of a backup."
    command_description = "Show the contents of a backup."

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        sub_parser = super(ShowSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument("backup_name",
            help="Purge backups older than this date time.")

        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):

        endpoint = self._endpoint(self.args)
        manifest = self._load_manifest(endpoint, self.args.backup_name)

        str_builder = ["Backup: %s" % manifest.backup_name]
        
        str_builder.append("")
        str_builder.append("Keyspace: %s:" % (manifest.keyspace,))
        str_builder.append("Host: %s:" % (manifest.host,))
        str_builder.append("Timestamp: %s:" % (manifest.timestamp,))
        
        for cf_name, cf_files in manifest.column_families.iteritems():
            str_builder.append("")
            str_builder.append("Column Family: %s" % (cf_name,))
            for cf_file in cf_files:
                str_builder.append("\t%s" % (cf_file,))
        
        return (0, "\n".join(str_builder))
