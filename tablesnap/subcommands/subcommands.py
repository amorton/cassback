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
import logging
import os
import os.path
import signal
import threading
import traceback

from tablesnap import cassandra
from tablesnap.endpoints import endpoints
# ============================================================================
# 

class SubCommand(object):
    """Base for all SubCommands that can be called on the command line. 

    :cls:`SubCommand` instances are created by the script entry point and 
    their ``__call__`` method called. 
    """

    command_name = None
    """Command line name for the Sub Command.

    Must be specified by sub classes.
    """

    command_help = None
    """Command line help for the Sub Command."""

    command_description = None
    """Command line description for the Sub Command."""


    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 

        Sub classes may override this method but pass the call up to ensure 
        the sub parser is created correctly. 

        A default ``func`` argument is set on the :cls:``ArgumentParser`` to 
        point to the constructor for the SubCommand class.

        Returns the :cls:`argparser.ArgumentParser`.
        """
        
        assert cls.command_name, "command_name must be set."

        parser = sub_parsers.add_parser(cls.command_name,
            help=cls.command_help or "No help", 
            description=cls.command_description or "No help", 
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.set_defaults(func=cls)
        return parser
    
    def __call__(self):
        """Called to execute the SubCommand.
        
        Must return a tuple of (rv, msg). Rv is returned as the script exit 
        and msg is the std out message.
        
        Must be implemented by sub classes.
        """
        raise NotImplementedError()
    
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Utility Functions
    
    def _endpoint(self, args):
        """Creates an endpoint from the command args."""
        return endpoints.create_from_args(args)
        
    def _list_manifests(self, endpoint, keyspace, host, day, 
        load_file_list=True):
        """List all the manifests names available for the ``keyspace`` and 
        ``host`` for the datetime ``day`` using the ``endpoint``.
        
        Returns a sorted list of the manifests.
        """
        
        manifest_dir = cassandra.KeyspaceBackup.backup_day_dir(keyspace, 
            host, day) 
        
        # Create a manifest from the file name that does not have the
        # full file list.
        manifests = [
            cassandra.KeyspaceBackup.from_backup_path(file_name)
            for file_name in endpoint.iter_dir(manifest_dir)
        ]
        manifests.sort(key=lambda x:x.timestamp)
        
        if load_file_list:
            # we want the list of files in the manifests.
            # so have to read it from disk
            return [
                endpoint.read_keyspace(manifest.backup_path)
                for manifest in manifests
            ]
        # OK to return empty manifests
        return manifests
        
    def _load_manifest_by_name(self, endpoint, backup_name):
        """Load the :cls:`cassandra.KeyspaceBackup` for the 
        backup with ``backup_name`` using the ``endpoint``.
        """

        ks_backup = cassandra.KeyspaceBackup.from_backup_name(
            backup_name)
        return endpoint.read_keyspace(ks_backup.backup_path)

class SubCommandWorkerThread(threading.Thread):
    """Base for threads used by sub commands.
    
    Provides a top level exception handler for the thread that kills the 
    process if :attr:`kill_on_error` is set. 
    
    Sub classes should implement :func:`_do_run` rather than :func:`run`.
    """
    
    def __init__(self, name, thread_id):
        super(SubCommandWorkerThread, self).__init__()

        self.name = "%s-%s" % (name, thread_id)
        self.daemon = True

        self.kill_on_error = True
        assert self.log, "Must have logger"
    
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Thread overrides.
    
    def run(self):

        try:
            self._do_run()
        except (Exception):
            msg = "Unexpected error in worker thread %s%s" % (
                self.name, " killing process" if self.kill_on_error else "")

            print msg
            print traceback.format_exc()
            self.log.critical(msg, exc_info=True)

            if self.kill_on_error:
                # Brute force kill self
                os.kill(os.getpid(), signal.SIGKILL)
    
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Abstract Methods
    
    def _do_run():
        """Called by :func:`run` to start the thread. 
        """
        pass

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # utility Methods
    
    def _endpoint(self, args):
        """Creates an endpoint from the command args."""
        return endpoints.create_from_args(args)
