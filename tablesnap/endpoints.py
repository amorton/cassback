
import argparse
import errno
import json
import logging
import os.path
import pkg_resources
import shutil

from tablesnap import file_util

# ============================================================================ 
#

def create_from_args(args):

    endpoint_name = args.endpoint
    for entry_point in pkg_resources.iter_entry_points("tablesnap.endpoints"):            
        
        endpoint_class = entry_point.load()
        if endpoint_class.name == endpoint_name:
            return endpoint_class(args)

    raise RuntimeError("Unknown endpoint name %(endpoint_name)s" % vars())

# ============================================================================ 
#

class EndpointBase(object):
    """Base for all endpoints."""

    name = None
    """Endpoint name, used in command line to identifity it. 
    """

    @classmethod
    def add_arg_group(cls, main_parser):
        """
        """
        pass

    def store_with_meta(self, source_path, source_meta, relative_dest_path, 
        ignore_if_existing=True):
        raise NotImplementedError()

    def read_meta(self, relative_path, ignore_missing=False):
        """Gets a dict of the meta data associated with the file at the 
        ``relative_path``.

        If ``ignore_missing`` and the file does not exist an empty dict is 
        returned. Otherwise an :exc:`EnvironmentError` with 
        :attr:`errno.ENOENT` as the errno is raised.
        """
        raise NotImplementedError()

    def store_json(self, data, relative_dest_path, ignore_if_existing=True):
        raise NotImplementedError()

    def read_json(self, relative_dest_path):
        raise NotImplementedError()

    def restore(self, relative_src_path, dest_path):
        """Restores the file in the backup at ``relative_src_path`` to the 
        path at ``dest_path``.
        """
        raise NotImplementedError()

    def exists(self, relative_path):
        """Returns ``True`` if the file at ``relative_path`` exists. False 
        otherwise. 
        """
        raise NotImplementedError()    

    def validate_checksum(self, relative_path, expected_md5_hex):
        """Validates that the MD5 checksum of the file in the backup at 
        ``relative_path`` matches ``expected_md5_hex``.  
        """
        raise NotImplementedError()


    def iter_dir(self, relative_path, include_files=True, 
        include_dirs=False, recursive=False):
        raise NotImplementedError()
    
    def remove_file(self, relative_path, ignore_missing=True):
        """Removes the file at the ``relative_path``. 
        
        Returns the fill path to the file in the backup."""
        raise NotImplementedError()

    def remove_file_with_meta(self, relative_path, ignore_missing=True):
        """Removes the file at the ``relative_path`` that is expected to 
        have meta data. 
        
        Returns the fill path to the file in the backup."""
        raise NotImplementedError()

        
# ============================================================================ 
#

class LocalEndpoint(EndpointBase):
    
    log = logging.getLogger("%s.%s" % (__name__, "LocalEndpoint"))
    name = "local"
    
    _META_SUFFIX = "-meta.json"
    @classmethod
    def add_arg_group(cls, main_parser):
        """
        """
        
        group = main_parser.add_argument_group("local endpoint", 
            description="Configuration for the local endpoint.")

        group.add_argument('--backup_base', default=None,
            help="Base destination path.")

        return group

    def __init__(self, args):
        self.args = args


    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Endpoint Base Overrides 

    def store_with_meta(self, source_path, source_meta, relative_dest_path, 
        ignore_if_existing=True):
        
        dest_path = self._safe_dest_path(relative_dest_path, 
            ignore_if_existing=ignore_if_existing)
        if not dest_path:
            return

        # Store the meta data first
        dest_meta_path = dest_path + self._META_SUFFIX
        with open(dest_meta_path, "w") as f:
            f.write(json.dumps(source_meta))
        
        # Store the actual file
        shutil.copy(source_path, dest_path)
        return

    def read_meta(self, relative_path, ignore_missing=False):

        path = os.path.join(self.args.backup_base, 
            relative_path + "-meta.json")
        try:
            with open(path, "r") as f:
                return json.loads(f.read())
        except (EnvironmentError) as e:
            if e.errno == errno.ENOENT and ignore_missing:
                # not found, just return None to say we could not load remote
                # file info
                self.log.debug("Ignoring missing meta file %(path)s" % vars())
                return {}
            raise


    def store_json(self, data, relative_dest_path, ignore_if_existing=True):

        dest_path = self._safe_dest_path(relative_dest_path, 
            ignore_if_existing=ignore_if_existing)
        if not dest_path:
            return

        with open(dest_path, "w") as f:
            f.write(json.dumps(data))
        return


    def restore(self, relative_src_path, dest_path):

        src_path = os.path.join(self.args.backup_base, relative_src_path)
        self.log.debug("Restoring file %(src_path)s to %(dest_path)s" % \
            vars())

        file_util.ensure_dir(dest_path)
        shutil.copy(src_path, dest_path)
        return

    def exists(self, relative_path):
        path = os.path.join(self.args.backup_base, relative_path)
        return os.path.exists(path)

    def validate_checksum(self, relative_path, expected_md5_hex):

        path = os.path.join(self.args.backup_base, relative_path)

        current_md5_hex, _  = file_util.file_md5(file_path)
        return current_md5_hex == expected_md5_hex

    def read_json(self, relative_path, ignore_missing=False):

        src_path = os.path.join(self.args.backup_base, relative_path)
        with open(src_path, "r") as f:
            return json.loads(f.read())

    def iter_dir(self, relative_path, include_files=True, 
        include_dirs=False, recursive=False):

        full_path = os.path.join(self.args.backup_base, relative_path)
        
        entries = []
        for root, dirs, files in os.walk(full_path):
            relative_root = root.replace(self.args.backup_base, "")
            if relative_root.startswith("/"):
                relative_root = relative_root[1:]
            
            if include_dirs:
                if recursive:
                    entries.extend(
                        os.path.join(relative_root, d)
                        for d in dirs
                    )
                else:
                    entries.extend(dirs)
                    
            if include_files:
                if recursive:
                    entries.extend(
                        os.path.join(relative_root, f)
                        for f in files
                        if not f.endswith(self._META_SUFFIX)
                    )
                else:
                    entries.extend(
                        f 
                        for f in files
                        if not f.startswith(self._META_SUFFIX)
                    )

            if not recursive:
                return entries
        return entries
        
    def remove_file(self, relative_path, ignore_missing=True):
        
        full_path = os.path.join(self.args.backup_base, relative_path)
        try:
            os.remove(full_path)
        except (EnvironmentError) as e:
            if e.errno == errno.ENOENT and ignore_missing:
                return None 
            raise

        file_util.maybe_remove_dirs(os.path.dirname(full_path))
        return full_path

    def remove_file_with_meta(self, relative_path, ignore_missing=True):
        
        # always try to delete meta data
        meta_path = relative_path + self._META_SUFFIX
        self.remove_file(meta_path, ignore_missing=True)
        
        return self.remove_file(relative_path, ignore_missing=ignore_missing)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Custom

    def _safe_dest_path(self, relative_dest_path, ignore_if_existing=True):
        dest_path = os.path.join(self.args.backup_base, relative_dest_path)

        if os.path.isfile(dest_path):
            if ignore_if_existing:
                self.log.warn("file %(dest_path)s exists skipping" % vars())
                return None
            else:
                raise RuntimeError("File %(dest_path)s exists" % vars())
        file_util.ensure_dir(dest_path)
        return dest_path

