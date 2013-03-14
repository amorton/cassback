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
import socket
import os.path

from tablesnap import cassandra, dt_util
from tablesnap.subcommands import subcommands

# ============================================================================
# List - used to list backups

class ListSubCommand(subcommands.SubCommand):
    """Base for Sub Commands that watch and backup files. 
    """

    log = logging.getLogger("%s.%s" % (__name__, "ListSubCommand"))

    command_name = "list"
    command_help = "List Backups"
    command_description = "List Backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """

        sub_parser = super(ListSubCommand, cls).add_sub_parser(sub_parsers)

        sub_parser.add_argument('--all', 
            action='store_true', dest='list_all', default=False,
            help="List all backups that match the criteria. Otherwise only "\
            "the most recent backup for the specified day is listed.")

        sub_parser.add_argument('--day', 
            default=dt_util.to_iso(dt_util.now_local()), 
            help="Day to return list the backups from.")
            
        sub_parser.add_argument("keyspace",
            help="Keyspace to list backups from.")
        sub_parser.add_argument('--host',
            default=socket.getfqdn(),  
            help="Host to list backups from. Defaults to the current host.")

        return sub_parser

    def __init__(self, args):
        self.args = args
        self.args.day = dt_util.to_utc(dt_util.parse_date_input(
            self.args.day))
        
    def __call__(self):

        self.log.info("Starting sub command %s", self.command_name)
        
        endpoint = self._endpoint(self.args)
        manifests = self._list_manifests(endpoint, self.args.keyspace,
            self.args.host, self.args.day, load_file_list=False)
            
        if not self.args.list_all and manifests:
            manifests = [max(manifests, key=lambda x:x.timestamp),]
        
        msg = "{prefix} for keyspace {keyspace} from {host} for {day} UTC:".format(
            prefix="All backups" if self.args.list_all else "Latest backup", 
            keyspace=self.args.keyspace, host=self.args.host, 
            day=self.args.day)
        buffer = [msg]
        
        if manifests:
            buffer.extend(
                manifest.backup_name 
                for manifest in manifests
            )
        else:
            buffer.append("None")
            
        self.log.info("Finished sub command %s", self.command_name)
        return (0, "\n".join(buffer)) 

