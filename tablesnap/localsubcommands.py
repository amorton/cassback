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

    def store(self, file_path):
        """Called up upload the file at ``file_path``.
        """
        
        meta = file_util.file_meta(file_path)

        endpoint_path = self.endpoint_path(file_path)
        if self.is_file_stored(endpoint_path, file_path, meta):
            self.log.warn("Endpoint path %(endpoint_path)s for file "\
                "%(file_path)s exists skipping" % vars())
            return

        if not self.snap_config.skip_index:
            index_json = json.dumps(file_util._file_index(file_path))
            self._do_store_index(index_json, endpoint_path)
        
        self._do_store(endpoint_path, file_path, meta)

        return

    def endpoint_path(self, file_path):
        
        if file_path.startswith("/"):
            file_path = file_path[1:]

        ep = os.path.join(self.local_config.dest_base, file_path)
        
        self.log.debug("Endpoint path for %(file_path)s is %(ep)s"\
            % vars())
        return ep

    def is_file_stored(self, endpoint_path, file_path, meta):
        return False

    def _do_store_index(self, index_json, endpoint_path):
        """
        """

        if self.snap_config.test_mode:
            self.log.info("TestMode - _do_upload_index %s" % vars())
            return
        index_path = "%(endpoint_path)s-listdir.json" % vars()

        self._ensure_dir(index_path)
        with open(index_path, "w") as f:
            f.write(index_json)
        return

    def _do_store(self, endpoint_path, file_path, file_meta):

        if self.snap_config.test_mode:
            self.log.info("TestMode - _do_store %s" % vars())
            return

        # Store the meta data
        meta_path = "%(endpoint_path)s-meta.json" % vars()

        self._ensure_dir(meta_path)
        with open(meta_path, "w") as f:
            f.write(json.dumps(file_meta))
        
        # copy the file
        shutil.copy(file_path, endpoint_path)

        return

    def _ensure_dir(self, path):
        if not os.path.isdir(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        return
    