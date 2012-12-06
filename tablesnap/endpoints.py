
import argparse
import cStringIO
import errno
import json
import logging
import os.path
import pkg_resources
import shutil
import time

import boto
from boto.s3 import key

from tablesnap import dt_util, file_util

# ============================================================================ 
#

def create_from_args(args):

    endpoint_name = args.endpoint
    for entry_point in pkg_resources.iter_entry_points("tablesnap.endpoints"):            
        
        endpoint_class = entry_point.load()
        if endpoint_class.name == endpoint_name:
            return endpoint_class(args)

    raise RuntimeError("Unknown endpoint name %(endpoint_name)s" % vars())

def validate_args(args):

    endpoint_name = args.endpoint
    for entry_point in pkg_resources.iter_entry_points("tablesnap.endpoints"):            
        
        endpoint_class = entry_point.load()
        if endpoint_class.name == endpoint_name:
            endpoint_class.validate_args(args)
            return

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
    
    @classmethod
    def validate_args(cls, args):
        pass
        
    def store_with_meta(self, source_path, source_meta, relative_dest_path):
        """Stores the local file at ``source_path`` at ``relative_dest_path`` 
        and included.
        
        Returns the fully qualified path to the file in the backup. 
        """
        raise NotImplementedError()

    def read_meta(self, relative_path):
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
        
        Returns the fully qualified backup path.
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
    
    def remove_file(self, relative_path):
        """Removes the file at the ``relative_path``. 
        
        Returns the fill path to the file in the backup."""
        raise NotImplementedError()

    def remove_file_with_meta(self, relative_path):
        """Removes the file at the ``relative_path`` that is expected to 
        have meta data. 
        
        Returns the fill path to the file in the backup."""
        raise NotImplementedError()


class TransferTiming(object):
    
    def __init__(self, logger, path, size):
        self.log = logger
        self.path = path
        self.start_ms = int(time.time() * 1000)
        self.size = size # bytes
        
        # number of boto callbacks we should ask for. 
        mb = 1024 **2
        pattern = [
            (1 * mb, 0),    # 1MB, none
            (10 * mb, 1),   # 10MB, 1
            (100 * mb, 2),  # 100MB, 2
            (1024 * mb, 5), # 1GB , 5
            (10 * 1024 * mb, ), # 10GB , 10
        ]
        
        self.num_callbacks = 20
        for i, j in pattern:
            if self.size < i:
                self.num_callbacks = j
                break

    def progress(self, progress, total):
        """Boto progress callback function. 
        
        Logs the progress. 
        """
        
        path = self.path
        elapsed_ms = int(time.time() * 1000) - self.start_ms
        throughput = ((progress * 1.0) / (1024**2)) / (elapsed_ms or 1)
        
        if progress == total:
            pattern = "Transfered file {path} in {elapsed_ms:d} ms size "\
            "{total} at {throughput:f} MB/sec"
        else:
            pattern = "Progress transfering file {path} elapsed "\
            "{elapsed_ms:d} ms, transferred "\
            "{progress} at {throughput:f} MB/sec {total} "\
            "total"

        self.log.debug(pattern.format(**vars()))
        return
    
    def __enter__(self):
        """Entry function when used as a context."""
        
        # Nothing to do. 
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        
        if exc_value is not None:
            # There was an error, let's just get out of here.
            return False
        
        # report 100% progress.
        self.progress(self.size, self.size)
        return False
        
# ============================================================================ 
# Local endpoint, mostly for testing. 

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

        group.add_argument('--backup-base', default=None, dest="backup_base",
            help="Base destination path.")

        return group

    def __init__(self, args):
        self.args = args


    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Endpoint Base Overrides 

    def store_with_meta(self, source_path, source_meta, relative_dest_path):
        
        dest_path = os.path.join(self.args.backup_base, relative_dest_path)
        file_util.ensure_dir(dest_path)
        
        # Store the actual file
        with TransferTiming(self.log, dest_path, source_meta["size"]):
            shutil.copy(source_path, dest_path)
        
        # Store the meta data
        dest_meta_path = dest_path + self._META_SUFFIX
        with open(dest_meta_path, "w") as f:
            f.write(json.dumps(source_meta))
        return dest_path

    def read_meta(self, relative_path):

        path = os.path.join(self.args.backup_base, 
            relative_path + "-meta.json")
        
        with open(path, "r") as f:
            return json.loads(f.read())

    def store_json(self, data, relative_dest_path, ignore_if_existing=True):

        dest_path = self._safe_dest_path(relative_dest_path, 
            ignore_if_existing=ignore_if_existing)
        if not dest_path:
            return
            z
        with open(dest_path, "w") as f:
            f.write(json.dumps(data))
        return


    def restore(self, relative_src_path, dest_path):

        src_path = os.path.join(self.args.backup_base, relative_src_path)
        self.log.debug("Restoring file %(src_path)s to %(dest_path)s" % \
            vars())
        
        size = os.stat(src_path).st_size
        with TransferTiming(self.log, src_path, size):
            shutil.copy(src_path, dest_path)
        return

    def exists(self, relative_path):
        path = os.path.join(self.args.backup_base, relative_path)
        return os.path.exists(path)

    def validate_checksum(self, relative_path, expected_md5_hex):

        path = os.path.join(self.args.backup_base, relative_path)

        current_md5, _ = file_util.file_md5(path)
        return current_md5 == expected_md5_hex

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
        
    def remove_file(self, relative_path):
        
        full_path = os.path.join(self.args.backup_base, relative_path)

        os.remove(full_path)
        file_util.maybe_remove_dirs(os.path.dirname(full_path))
        return full_path

    def remove_file_with_meta(self, relative_path):
        
        # always try to delete meta data
        meta_path = relative_path + self._META_SUFFIX
        try:
            self.remove_file(meta_path)
        except (EnvironmentError) as e:
            if not (e.errno == errno.ENOENT):
                raise
        return self.remove_file(relative_path)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Custom

    def _safe_dest_path(self, relative_dest_path, ignore_if_existing=True):
        dest_path = os.path.join(self.args.backup_base, relative_dest_path)

        if os.path.isfile(dest_path):
            #TODO: check MD5
            if ignore_if_existing:
                return None
            else:
                raise RuntimeError("File %(dest_path)s exists" % vars())
        file_util.ensure_dir(dest_path)
        return dest_path

# ============================================================================ 
# S3 endpoint

class S3Endpoint(EndpointBase):
    
    log = logging.getLogger("%s.%s" % (__name__, "S3Endpoint"))
    name = "s3"

    def __init__(self, args):
        self.args = args
        

        self.log.info("Creating S3 connection.")
        self.s3_conn = boto.connect_s3(self.args.aws_key, 
            self.args.aws_secret)
        self.s3_conn.retries = self.args.retries
        
        self.log.debug("Creating S3 bucket %(bucket_name)s" % vars(self.args))
        self.bucket = self.s3_conn.get_bucket(self.args.bucket_name)

        
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Endpoint Base Overrides 
    
    @classmethod
    def add_arg_group(cls, main_parser):

        group = main_parser.add_argument_group("S3 endpoint", 
            description="Configuration for the AWS S3 endpoint.")

        group.add_argument('--aws-key', dest='aws_key', default=None,
            help="AWS API Key")
        group.add_argument('--aws-secret', dest='aws_secret', 
            default=None, help="AWS API Secret Key")
        group.add_argument('--bucket-name', default=None, dest="bucket_name",
            help='S3 bucket to upload to.')
            
        group.add_argument('--max-upload-size-mb', dest='max_upload_size_mb', 
            type=int, default=5120,
            help='Max size for files to be uploaded before doing multipart ')
        group.add_argument('--multipart-chunk-size-mb', 
            dest='multipart_chunk_size_mb', default=256, type=int,
            help='Chunk size for multipart uploads (10%% of '
            'free memory if default is not available)')
        group.add_argument('--retries', 
            dest='retries', default=5, type=int,
            help='Number of times to retry s3 calls')

        return group
    
    @classmethod
    def validate_args(cls, args):
        
        if args.multipart_chunk_size_mb < 5:
            # S3 has a minimum. 
            raise argparse.ArgumentTypeError("Minimum "\
                "multipart_chunk_size_mb value is 5.")
        return

    def store_with_meta(self, source_path, source_meta, relative_dest_path):

        is_multipart_upload = source_meta["size"] > \
            (self.args.max_upload_size_mb * (1024**2))
        
        if is_multipart_upload:
            path = self._do_multi_part_upload(source_path, source_meta, 
                relative_dest_path)
        else:
            path = self._do_single_part_upload(source_path, source_meta, 
            relative_dest_path)
        
        return path
        
    def read_meta(self, relative_path):
        
        key_name = relative_path
        fqn = self._fqn(key_name)

        self.log.debug("Starting to read meta for key %(fqn)s " % vars())
      
        key = self.bucket.get_key(key_name)
        if key is None:
            raise EnvironmentError(errno.ENOENT, fqn)
        
        self.log.debug("Finished reading meta for key %(fqn)s " % vars())
        return key.metadata

    def store_json(self, data, relative_dest_path):

        key_name = relative_dest_path
        fqn = self._fqn(key_name)
        
        self.log.debug("Starting to store json to %(fqn)s" % vars())
        
        # TODO: Overwrite ? 
        key = self.bucket.new_key(key_name)
        json_str = json.dumps(data)
        timing = TransferTiming(self.log, fqn, len(json_str))
        key.set_contents_from_string(
            json_str,
            headers={'Content-Type': 'application/json'}, 
            cb=timing.progress, num_cb=timing.num_callbacks)

        self.log.debug("Finished storing json to %(fqn)s" % vars())
        return 
        

    def read_json(self, relative_dest_path):
        
        key_name = relative_dest_path
        fqn = self._fqn(key_name)
        
        self.log.debug("Starting to read json from %(fqn)s" % vars())
        
        key = self.bucket.get_key(key_name)
        if key is None:
            raise EnvironmentError(errno.ENOENT, fqn)
        timing = TransferTiming(self.log, fqn, 0)
        data = json.loads(key.get_contents_as_string(cb=timing.progress, 
            num_cb=timing.num_callbacks))
        self.log.debug("Finished reading json from %(fqn)s" % vars())
        return data
        
    def restore(self, relative_src_path, dest_path):
        """Restores the file in the backup at ``relative_src_path`` to the 
        path at ``dest_path``.
        """
        
        key_name = relative_src_path
        fqn = self._fqn(key_name)
        
        self.log.debug("Starting to restore from %(fqn)s to %(dest_path)s" \
            % vars())
        
        key = self.bucket.get_key(key_name)
        if key is None:
            raise EnvironmentError(errno.ENOENT, fqn)
        timing = TransferTiming(self.log, fqn, int(key.metadata["size"]))
        key.get_contents_to_filename(dest_path, cb=timing.progress, 
            num_cb=timing.num_callbacks)
        timing.report()
        
        return key_name

    def exists(self, relative_path):
        """Returns ``True`` if the file at ``relative_path`` exists. False 
        otherwise. 
        """
        
        key_name = relative_path
        fqn = self._fqn(key_name)
        
        self.log.debug("Checking if key %(fqn)s exists" % vars())
        key = self.bucket.get_key(key_name)
        return False if key is None else True 
            

    def validate_checksum(self, relative_path, expected_hash):
        """Validates that the MD5 checksum of the file in the backup at 
        ``relative_path`` matches ``expected_md5_hex``.  
        """
        
        key_name = relative_path
        fqn = self._fqn(key_name)

        self.log.debug("Starting to validate checkum for %(fqn)s" % vars())
            
        key = self.bucket.get_key(key_name)
        if key == None: 
            self.log.debug("Key %(fqn)s does not exist, is checksum is "\
                "invalid" % vars())
            return False

        # original checked size, not any more.
        key_md5 = key.get_metadata('md5sum')
        if key_md5:
            hash_match = expected_hash == key_md5
            log_func = self.log.debug if hash_match else self.log.warn
            log_func("%s with key %s hash %s and expected hash %s" % (
                "Match" if hash_match else "Mismatch", fqn, key_md5, 
                expected_hash))
            return hash_match

        key_etag = key.etag.strip('"')
        hash_match = expected_hash == key_etag
        log_func = self.log.debug if hash_match else self.log.warn
        log_func("%s with key %s etag %s and expected hash %s" % (
            "Match" if hash_match else "Mismatch", fqn,key_etag, 
            expected_hash))
            
        return hash_match

    def iter_dir(self, relative_path, include_files=True, 
        include_dirs=False, recursive=False):
        
        key_name = relative_path
        if not key_name.endswith("/"):
            key_name = key_name + "/"
        fqn = self._fqn(key_name)
        
        self.log.debug("Starting to iterate the dir for %(fqn)s" % vars())
        
        if include_files and not include_dirs and not recursive:
            # easier, we just want to list the keys. 
            return [
                key.name.replace(key_name, "")
                for key in self.bucket.list(prefix=key_name)
            ]
        
        items = []
        
        if not recursive:
            # return files and/or directories in this path
            for entry in self.bucket.list(prefix=key_name, delimiter="/"):
                if include_files and isinstance(entry, key.Key):
                    items.append(entry.name.replace(key_name, ""))
                elif include_dirs:
                    items.append(entry.name.replace(key_name, ""))
            return items
        
        # recursive, we need to do a hierarchal list
        def _walk_keys(inner_key):
            for entry in self.bucket.list(prefix=key_name, delimiter="/"):
                if isinstance(entry, key.Key):
                    yield entry.name
                else:
                    # this is a directory. 
                    yield entry.name
                    for sub_entry in _walk_keys(entry.name):
                        yield sub_entry
        return list(_walk_keys(key_name))
    
    def remove_file(self, relative_path):
        """Removes the file at the ``relative_path``. 
        
        Returns the fill path to the file in the backup."""
        
        key_name = relative_path
        bucket_name = self.args.bucket_name
        
        self.log.debug("Starting to delete key %(key_name)s in "\
            "%(bucket_name)s" % vars())
        
        s3_key = self._bucket().key(key_name)
        if key is None:
            self.log.debug("Key %(key_name)s was already deleted." % vars())
            return
        s3_key.delete()

        self.log.debug("Finished deleting from %(key_name)s in "\
            "%(bucket_name)s" % vars())
        return key_name

    def remove_file_with_meta(self, relative_path):
        """Removes the file at the ``relative_path`` that is expected to 
        have meta data. 
        
        Returns the fill path to the file in the backup."""
        
        # In S3 the meta is stored with the key. 
        return self.remove_file(relative_path)
        

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Custom
    
    def _fqn(self, key_name):
        """Returns fully qualified name for the bucket and key.
        
        Note the fully qualified name is not a url. It has the form 
        <bucket_name>//key_path"""
        
        return  "%s//%s" % (self.args.bucket_name, key_name)
        
    def _do_multi_part_upload(self, source_path, source_meta, key_name):
        
        fqn = self._fqn(key_name)
        self.log.debug("Starting multi part upload of %(source_path)s to "\
            "%(fqn)s" % vars())
        # All meta tags must be strings
        metadata = {
            k : str(v)
            for k,v in source_meta.iteritems() 
        }
        mp = self.bucket.initiate_multipart_upload(key_name, 
            metadata=metadata)
        
        timing = TransferTiming(self.log, fqn, source_meta["size"])
        chunk = None
        try:
            # Part numbers must start at 1self.
            for part, chunk in enumerate(self._chunk_file(source_path),1):
                self.log.debug("Uploading part %(part)s with" % vars())
                try:
                    mp.upload_part_from_file(chunk, part, cb=timing.progress, 
                        num_cb=timing.num_callbacks)
                finally:
                    chunk.close()
        except (Exception):
            mp.cancel_upload()
            raise

        mp.complete_upload()
        self.log.debug("Finished multi part upload of %(source_path)s to "\
            "%(fqn)s" % vars())
            
        return fqn

    def _do_single_part_upload(self, source_path, source_meta, key_name):
        
        fqn = self._fqn(key_name)
        self.log.debug("Starting single part upload of %(source_path)s to "\
            "%(fqn)s" % vars())
            
        key = self.bucket.new_key(key_name)
        
        # All meta data fields have to be strings.
        key.update_metadata({
            k : str(v)
            for k, v in source_meta.iteritems()
        })
        
        # Rebuild the MD5 tuple boto makes
        md5 = (
            source_meta["md5_hex"], 
            source_meta["md5_base64"], 
            source_meta["size"]
        )
        timing = TransferTiming(self.log, fqn, source_meta["size"])
        key.set_contents_from_filename(source_path, replace=False, md5=md5, 
            cb=timing, num_cb=timing.num_callbacks)

        self.log.debug("Finished single part upload of %(source_path)s to "\
            "%(fqn)s" % vars())
        return fqn
    
    def _chunk_file(self, file_path):
        """Yield chunks from ``file_path``.
        """
        
        chunk_bytes = self.args.multipart_chunk_size_mb * (1024**2)
        
        self.log.debug("Splitting file %(file_path)s into chunks of "\
            "%(chunk_bytes)s bytes" % vars())

        with open(file_path, 'rb') as f:
            chunk = f.read(chunk_bytes)
            while chunk:
                yield cStringIO.StringIO(chunk)
                chunk = f.read(chunk_bytes)
        return

# ============================================================================ 
# Survey endpoint writes all operations to a file without doing anything 
# Used to estimate S3 billing etc. 

class SurveyEndpoint(EndpointBase):
    
    log = logging.getLogger("%s.%s" % (__name__, "SurveyEndpoint"))
    name = "survey"
    
    FILE_OP_PATTERN = "{time} {action} {src_path} to {dest_path} "\
        "size {bytes}\n"
    
    def __init__(self, args):
        self.args = args
        
        self.log_file = open(self.args.survey_path, "a+")
        
    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Endpoint Base Overrides

    @classmethod
    def add_arg_group(cls, main_parser):
        """
        """
        
        group = main_parser.add_argument_group("survey endpoint", 
            description="Configuration for the survey endpoint.")

        group.add_argument('--survey-path', dest="survey_path",
            help="Path to the survey log file.")
        group.add_argument('--meta-dir', dest="meta_dir",
            help="Directory to store meta data files in.")
        return group

    @classmethod
    def validate_args(cls, args):
        
        if os.path.isfile(args.survey_path):
            return
        
        if os.path.exists(args.survey_path):
            raise argparse.ArgumentError("--survey-path",
                "Path exists and is not a file.")
        
        file_util.ensure_dir(os.path.dirname(args.survey_path))
        file_util.ensure_dir(args.meta_dir)
        return
        
    def store_with_meta(self, source_path, source_meta, relative_dest_path):
        """Writes the meta data and upates the survey log."""
        
        msg = self.FILE_OP_PATTERN.format(time=dt_util.now_iso(), 
            action="stored", src_path=source_path, 
            dest_path=relative_dest_path, 
            bytes=source_meta["size"])
        self.log_file.write(msg)
        self.log_file.flush()
        
        dest_path = os.path.join(self.args.meta_dir, relative_dest_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        with open(dest_path, "w") as f:
            f.write(json.dumps(source_meta))
        return relative_dest_path

    def read_meta(self, relative_path):

        path = os.path.join(self.args.meta_dir, relative_path)
        with open(path, "r") as f:
            return json.loads(f.read())

    def store_json(self, data, relative_dest_path, ignore_if_existing=True):
        
        dest_path = os.path.join(self.args.meta_dir, relative_dest_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        with open(dest_path, "w") as f:
            f.write(json.dumps(data))
        return


    def restore(self, relative_src_path, dest_path):
        
        src_meta = self.read_meta(relative_src_path)
        msg = self.FILE_OP_PATTERN.format(time=dt_util.now_iso(), 
            action="restored", src_path=relative_src_path, 
            dest_path=dest_path, 
            bytes=src_meta["size"])
        self.log_file.write(msg)
        self.log_file.flush()
        return

    def exists(self, relative_path):
        path = os.path.join(self.args.meta_dir, relative_path)
        return os.path.exists(path)

    def validate_checksum(self, relative_path, expected_md5_hex):
        return True

    def read_json(self, relative_path, ignore_missing=False):

        src_path = os.path.join(self.args.meta_dir, relative_path)
        with open(src_path, "r") as f:
            return json.loads(f.read())

    def iter_dir(self, relative_path, include_files=True, 
        include_dirs=False, recursive=False):

        full_path = os.path.join(self.args.meta_dir, relative_path)
        
        entries = []
        for root, dirs, files in os.walk(full_path):
            relative_root = root.replace(self.args.meta_dir, "")
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
                    )
                else:
                    entries.extend(
                        f 
                        for f in files
                    )

            if not recursive:
                return entries
        return entries
        
    def remove_file(self, relative_path):
        
        src_meta = self.read_meta(relative_path)
        size = src_meta.get("size")
        # If no size do not add to the survey, 
        # it's probably not a file with meta data.
        # e.g. we are deleting a manifest
        if size is not None:
            msg = self.FILE_OP_PATTERN.format(time=dt_util.now_iso(), 
                action="removed", src_path=relative_path, 
                dest_path="na", 
                bytes=size)
            
            self.log_file.write(msg)
            self.log_file.flush()
        
        path = os.path.join(self.args.meta_dir, relative_path)
        os.remove(path)
        file_util.maybe_remove_dirs(os.path.dirname(path))
        return relative_path

    def remove_file_with_meta(self, relative_path):
        
        return self.remove_file(relative_path)

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # 
    