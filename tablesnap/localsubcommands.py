"""Sub Commands copy things locally
"""
import copy
import errno
import json
import logging
import os
import shutil
import socket

import file_util, snapsubcommands

class LocalConfig(object):
    """Local config. 
    """

    def __init__(self, dest_base):

        self.dest_base = dest_base

    @classmethod    
    def from_args(cls, args):
        return LocalConfig(args.dest_base)

class LocalSnapSubCommand(snapsubcommands.SnapSubCommand):
    """SubCommand to store SSTables locally
    """

    log = logging.getLogger("%s.%s" % (__name__, "LocalSnapSubCommand"))

    # command description used by the base 
    command_name = "snap-local"
    command_help = "Copy new SSTables to locally"
    command_description = "Copy new SSTables to locally"


    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(LocalSnapSubCommand, cls).add_sub_parser(sub_parsers)

        parser.add_argument('dest_base', default=None,
            help="Base destination path.")

        return parser

    def __init__(self, args):
        super(LocalSnapSubCommand, self).__init__(args)
        self.local_config = LocalConfig.from_args(args)

    def _create_endpoint(self):
        return LocalEndpoint(copy.deepcopy(self.snap_config),
            copy.deepcopy(self.local_config))


class LocalEndpoint(object):
    log = logging.getLogger("%s.%s" % (__name__, "LocalEndpoint"))

    def __init__(self, snap_config, local_config):

        self.snap_config = snap_config
        self.local_config = local_config

    def store(self, ks_manifest, cass_file):
        """Called up upload the ``cass_file``.
        """
        
        if self.is_file_stored(cass_file):
            self.log.warn("Endpoint path %(endpoint_path)s for file "\
                "%(cass_file)s exists skipping" % vars())
            return

        # Store the keyspace manifest
        dest_manifest_path = os.path.join(self.local_config.dest_base, 
            ks_manifest.backup_path())

        if self.snap_config.test_mode:
            self.log.info("TestMode -  store keyspace manifest to "\
                "%(dest_manifest_path)s" % vars())
        else:
            self._ensure_dir(dest_manifest_path)
            with open(dest_manifest_path, "w") as f:
                f.write(json.dumps(ks_manifest.manifest))
        
        # Store the cassandra file
        dest_file_path = os.path.join(self.local_config.dest_base, 
            cass_file.backup_path())

        if self.snap_config.test_mode:
            self.log.info("TestMode - store file to %(dest_file_path)s"\
                 % vars())
        else:
            # Store the file
            dest_meta_path = "%(dest_file_path)s-meta.json" % vars()

            self._ensure_dir(dest_meta_path)
            with open(dest_meta_path, "w") as f:
                f.write(json.dumps(cass_file.file_meta))
            
            # copy the file
            shutil.copy(cass_file.file_path, dest_file_path)
        return


    def is_file_stored(self, cass_file):
        # HACK: 
        return False

    def _ensure_dir(self, path):
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        return
    