"""Sub Commands copy things locally
"""
import copy
import errno
import json
import logging
import os
import shutil
import socket

import file_util, cassandra, subcommands


# ============================================================================
#

class LocalSnapSubCommand(subcommands.SnapSubCommand):
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
        parser.add_argument('backup_base', default=None,
            help="Base destination path.")

        return parser

    def _create_worker_thread(self, i, file_queue, args):
        return LocalSnapWorkerThread(i, file_queue, args)

class LocalSnapWorkerThread(subcommands.SnapWorkerThread):
    log = logging.getLogger("%s.%s" % (__name__, "LocalSnapWorkerThread"))

    def _store(self, ks_manifest, cass_file):
        """Called up upload the ``cass_file``.
        """
        
        if self.is_file_stored(cass_file):
            # CRAP:
            self.log.warn("file "\
                "%(cass_file)s exists skipping" % vars())
            return

        # Store the keyspace manifest
        dest_manifest_path = os.path.join(self.args.backup_base, 
            ks_manifest.backup_path)

        if self.args.test_mode:
            self.log.info("TestMode -  store keyspace manifest to "\
                "%(dest_manifest_path)s" % vars())
        else:
            self._ensure_dir(dest_manifest_path)
            with open(dest_manifest_path, "w") as f:
                f.write(json.dumps(ks_manifest.to_manifest()))
        
        # Store the cassandra file
        dest_file_path = os.path.join(self.args.backup_base, 
            cass_file.backup_path)

        if self.args.test_mode:
            self.log.info("TestMode - store file to %(dest_file_path)s"\
                 % vars())
        else:
            # Store the file
            dest_meta_path = "%(dest_file_path)s-meta.json" % vars()

            self._ensure_dir(dest_meta_path)
            with open(dest_meta_path, "w") as f:
                f.write(json.dumps(cass_file.meta))
            
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
    

# ============================================================================
#

class LocalListSubCommand(subcommands.ListSubCommand):
    """SubCommand to list backups
    """

    log = logging.getLogger("%s.%s" % (__name__, "LocalListSubCommand"))

    # command description used by the base 
    command_name = "list-local"
    command_help = "List local backup files"
    command_description = "List local backup files"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(LocalListSubCommand, cls).add_sub_parser(sub_parsers)
        parser.add_argument('backup_base', default=None,
            help="Base destination path.")

        return parser


    def _list_manifests(self):

        dest_manifest_path = os.path.join(self.args.backup_base, 
            cassandra.KeyspaceManifest.backup_dir(self.args.keyspace))

        _, _, all_files = os.walk(dest_manifest_path).next()

        host_manifests = []
        for f in all_files:
            backup_name, _ = os.path.splitext(f)
            manifest = cassandra.KeyspaceManifest.from_backup_name(
                backup_name)
            if manifest.host == self.args.host:
                host_manifests.append(f)

        if self.args.list_all:
            return host_manifests

        return [max(host_manifests),]

# ============================================================================
#

class LocalValidateSubCommand(subcommands.ValidateSubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "LocalValidateSubCommand"))

    # command description used by the base 
    command_name = "validate-local"
    command_help = "Validate local backups"
    command_description = "Validatelocal backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(LocalValidateSubCommand, cls).add_sub_parser(sub_parsers)
        parser.add_argument('backup_base', default=None,
            help="Base destination path.")

        return parser


    def _load_manifest(self):

        empty_manifest = cassandra.KeyspaceManifest.from_backup_name(
            self.args.backup_name)

        dest_manifest_path = os.path.join(self.args.backup_base, 
            empty_manifest.backup_path)

        with open(dest_manifest_path, "r") as f:
            return cassandra.KeyspaceManifest.from_manifest(
                json.loads(f.read()))

    def _load_remote_file_info(self, host, file_name):

        cass_file = cassandra.CassandraFile.from_file_path(file_name, meta={}, 
            host=host)

        meta_path = os.path.join(*(
            self.args.backup_base, 
            cass_file.backup_path + "-meta.json"))
        try:
            with open(meta_path, "r") as f:
                cass_file.meta = json.loads(f.read())
        except (EnvironmentError) as e:
            if e.errno == errno.ENOENT:
                # not found, just return None to say we could not load remote
                # file info
                return None

        return cass_file


    def _file_exists(self, backup_file):

        file_path = os.path.join(self.args.backup_base, 
            backup_file.backup_path)

        return os.path.isfile(file_path)

    def _checksum_file(self, backup_file):


        backup_md5_hex = backup_file.meta.get("md5_hex")

        file_path = os.path.join(self.args.backup_base, 
            backup_file.backup_path)

        current_md5_hex, _  = file_util.file_md5(file_path)
        return current_md5_hex == backup_md5_hex

