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

from cassback import dt_util, file_util
from cassback.subcommands import subcommands

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
        ks_backup = self._load_manifest_by_name(endpoint, 
            self.args.backup_name)

        str_builder = ["Backup: %s" % ks_backup.backup_name]
        
        str_builder.append("")
        str_builder.append("Keyspace: %s:" % (ks_backup.keyspace,))
        str_builder.append("Host: %s:" % (ks_backup.host,))
        str_builder.append("Timestamp: %s:" % (ks_backup.timestamp,))
        total_size = sum(c.stat.size for c in ks_backup.iter_components())
        str_builder.append("Size: %s / %s" % (total_size, 
            file_util.human_disk_bytes(total_size)))
        
        last_component = None
        file_str = []
        # iterates in order
        for component in ks_backup.iter_components():
            if not last_component or (component.cf != last_component.cf):
                str_builder.append("")
                str_builder.append("Column Family: %s" % (component.cf,))
                last_component = component
            
            str_builder.append("\t{c.backup_file_name} "\
                "{c.stat.size}/{human_size}".format(c=component, 
                human_size=file_util.human_disk_bytes(component.stat.size)))
        
        return (0, "\n".join(str_builder))
