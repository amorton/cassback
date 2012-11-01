"""Sub Commands copy things locally
"""
import argparse
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
            file_util.ensure_dir(dest_manifest_path)
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

            file_util.ensure_dir(dest_meta_path)
            with open(dest_meta_path, "w") as f:
                f.write(json.dumps(cass_file.meta))
            
            # copy the file
            assert cass_file.original_path
            shutil.copy(cass_file.original_path, dest_file_path)
        return


    def is_file_stored(self, cass_file):
        # HACK: 
        return False

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

# ============================================================================
#

class LocalSlurpSubCommand(subcommands.SlurpSubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "LocalSlurpSubCommand"))

    # command description used by the base 
    command_name = "slurp-local"
    command_help = "Restore local backups"
    command_description = "Restore local backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(LocalSlurpSubCommand, cls).add_sub_parser(sub_parsers)
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

    def _create_validation_cmd(self):

        validate_args = argparse.Namespace(
            backup_name=self.args.backup_name,
            backup_base=self.args.backup_base,
            checksum=self.args.checksum
        )

        return LocalValidateSubCommand(validate_args)

    def _create_worker_thread(self, thread_id, work_queue, result_queue,args):
        
        return LocalSlurpWorkerThread(thread_id, work_queue, result_queue, 
            args)

class LocalSlurpWorkerThread(subcommands.SlurpWorkerThread):
    log = logging.getLogger("%s.%s" % (__name__, "LocalSlurpWorkerThread"))


    def _restore_file(self, cass_file, dest_path):
        
        src_path = os.path.join(self.args.backup_base, 
            cass_file.backup_path)
        self.log.debug("Restoring file %(cass_file)s from %(src_path)s to "\
            "%(dest_path)s" % vars())

        file_util.ensure_dir(dest_path)
        shutil.copy(src_path, dest_path)
        return


# ============================================================================
#

class LocalPurgeSubCommand(subcommands.PurgeSubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "LocalPurgeSubCommand"))

    # command description used by the base 
    command_name = "purge-local"
    command_help = "Purge local backups"
    command_description = "Purge local backups"

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """Called to add a parser to ``sub_parsers`` for this command. 
        """
        
        parser = super(LocalPurgeSubCommand, cls).add_sub_parser(sub_parsers)
        parser.add_argument('backup_base', default=None,
            help="Base destination path.")

        return parser


    def _all_manifests(self, keyspace, host, all_hosts):
        """Called to load all the manifests for the ``keyspace`` and host(s)
        combinations. 
        """
        
        # Step 1 - get the keyspace dirs
        # if keyspace arg is empty then get all. 
        if keyspace:
            keyspace_dirs = [
                os.path.join(self.args.backup_base, "cluster", keyspace)
            ]
        else:
            keyspace_dirs = file_util.list_dirs(os.path.join(
                self.args.backup_base, "cluster"))

        # Step 2 - Load the manifests
        manifests = []
        for manifest_path in file_util.list_files(keyspace_dirs):
            # Just load the manifest and check the host
            # could be better. 
            self.log.debug("Opening manifest file %(manifest_path)s" \
                % vars())
            with open(manifest_path, "r") as f:
                data = json.loads(f.read())
            manifest = cassandra.KeyspaceManifest.from_manifest(data)

            if all_hosts or manifest.host == host:
                manifests.append(manifest)

        return manifests


    def _purge_manifests(self, purged_manifests):
        """Called to delete the manifest files for ``purged_manifests``.
        """
        
        deleted = []
        for manifest in purged_manifests:
            path = os.path.join(self.args.backup_base, manifest.backup_path)

            self.log.debug("Purging manifest file %(path)s" % vars())
            os.remove(path)
            file_util.maybe_remove_dirs(os.path.dirname(path))
            deleted.append(path)
        return deleted


    def _purge_files(self, keyspace, host, all_hosts, kept_files):
        """Called to delete the files for the ``keyspace`` and hosts 
        combinations that are not listed in ``kept_files``. 
        """
        
        # Step 1 - top level host directories in the backup
        if all_hosts:
            host_dirs = file_util.list_dirs(os.path.join(
                self.args.backup_base, "hosts"))
        else:
            host_dirs = [os.path.join(self.args.backup_base, "hosts", host)]


        # Step 2 - keyspace directories
        # if empty read all
        if keyspace:
            keyspace_dirs = [
                os.path.join(dir, keyspace)
                for dir in host_dirs
            ]
        else:
            keyspace_dirs = []
            keyspace_dirs.extend(file_util.list_dirs(host_dirs))

        # Step 3 - delete things. 
        def yield_paths():
            # TODO: check the version !
            for keyspace_dir in keyspace_dirs:
                # First level is the CF name
                for cf_dir in file_util.list_dirs(keyspace_dir):
                    for file_path in file_util.list_files(cf_dir):
                        yield file_path

        deleted_files = []
        for file_path in yield_paths():
            _, file_name = os.path.split(file_path)

            # storing file meta in a json file on disk is implementation 
            # detail for the *-local sub commands.
            if file_name.endswith("-meta.json"):
                # keep if the sstable file is being kept. 
                keep_file = file_name.rstrip("-meta.json") in kept_files
            else:
                keep_file = file_name in kept_files

            if keep_file:
                self.log.debug("Keeping file %(file_path)s" % vars())
            else:
                self.log.debug("Deleting file %(file_path)s" % vars())
                os.remove(file_path)
                deleted_files.append(file_path)
                file_util.maybe_remove_dirs(os.path.dirname(file_path))

        return deleted_files















