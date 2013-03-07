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
import cStringIO
import errno
import json
import logging
import numbers
import os.path

import boto
from boto.s3 import key as s3_key
from boto.s3 import prefix as s3_prefix

from tablesnap import cassandra, file_util
from tablesnap.endpoints import endpoints

# ============================================================================ 
# S3 endpoint

class S3Endpoint(endpoints.EndpointBase):
    
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
        group.add_argument('--key-prefix', default="", dest="key_prefix",
            help='S3 key prefix.')
            
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

    def backup_file(self, backup_file):
        
        is_multipart_upload = backup_file.component.stat.size > (
            self.args.max_upload_size_mb * (1024**2))
        
        if is_multipart_upload:
            path = self._do_multi_part_upload(backup_file)
        else:
            path = self._do_single_part_upload(backup_file)
        
        return path
        
    def read_backup_file(self, path):
        
        key_name = path
        fqn = self._fqn(key_name)

        self.log.debug("Starting to read meta for key %s:%s ", 
            self.args.bucket_name, fqn)
      
        key = self.bucket.get_key(fqn)
        if key is None:
            raise EnvironmentError(errno.ENOENT, fqn)
        
        self.log.debug("Finished reading meta for key %s:%s ", 
            self.args.bucket_name, fqn)
        return cassandra.BackupFile.deserialise(self._aws_meta_to_dict(
            key.metadata))

    def backup_keyspace(self, ks_backup):

        key_name = ks_backup.backup_path
        fqn = self._fqn(key_name)
        
        self.log.debug("Starting to store json to %s:%s", 
            self.args.bucket_name, fqn)
        
        # TODO: Overwrite ? 
        key = self.bucket.new_key(fqn)
        json_str = json.dumps(ks_backup.serialise())
        timing = endpoints.TransferTiming(self.log, fqn, len(json_str))
        key.set_contents_from_string(
            json_str,
            headers={'Content-Type': 'application/json'}, 
            cb=timing.progress, num_cb=timing.num_callbacks)

        self.log.debug("Finished storing json to %s:%s", 
            self.args.bucket_name, fqn)
        return 
        

    def read_keyspace(self, path):
        
        key_name = path
        fqn = self._fqn(key_name)
        
        self.log.debug("Starting to read json from %s:%s", 
            self.args.bucket_name, fqn)
        
        key = self.bucket.get_key(fqn)
        if key is None:
            raise EnvironmentError(errno.ENOENT, fqn)
        timing = endpoints.TransferTiming(self.log, fqn, 0)
        data = json.loads(key.get_contents_as_string(cb=timing.progress, 
            num_cb=timing.num_callbacks))
        self.log.debug("Finished reading json from %s:%s", 
            self.args.bucket_name, fqn)
        
        return cassandra.KeyspaceBackup.deserialise(data)
        
    def restore_file(self, backup_file, dest_prefix):
        """
        """
        
        key_name = backup_file.backup_path
        fqn = self._fqn(key_name)
        dest_path = os.path.join(dest_prefix, backup_file.restore_path)
        file_util.ensure_dir(os.path.dirname(dest_path))
        self.log.debug("Starting to restore from %s:%s to %s", 
            self.args.bucket_name, fqn, dest_path)
        
        key = self.bucket.get_key(fqn)
        if key is None:
            raise EnvironmentError(errno.ENOENT, fqn)
        timing = endpoints.TransferTiming(self.log, fqn, 
            backup_file.component.stat.size)
        key.get_contents_to_filename(dest_path, cb=timing.progress, 
            num_cb=timing.num_callbacks)
        
        return dest_path

    def exists(self, relative_path):
        """Returns ``True`` if the file at ``relative_path`` exists. False 
        otherwise. 
        """
        
        key_name = relative_path
        fqn = self._fqn(key_name)
        
        self.log.debug("Checking if key %s:%s exists", self.args.bucket_name, 
            fqn)
        key = self.bucket.get_key(fqn)
        return False if key is None else True 
            

    def validate_checksum(self, relative_path, expected_hash):
        """Validates that the MD5 checksum of the file in the backup at 
        ``relative_path`` matches ``expected_md5_hex``.  
        """
        
        key_name = relative_path
        fqn = self._fqn(key_name)

        self.log.debug("Starting to validate checkum for %s:%s", 
            self.args.bucket_name, fqn)
            
        key = self.bucket.get_key(fqn)
        if key == None: 
            self.log.debug("Key %s does not exist, so checksum is invalid", 
                fqn)
            return False

        # original checked size, not any more.
        key_md5 = key.get_metadata('md5sum')
        if key_md5:
            hash_match = expected_hash == key_md5
        else:
            key_etag = key.etag.strip('"')
            self.log.info("Missing md5 meta data for %s using etag", fqn)
            hash_match = expected_hash == key_etag
        
        if hash_match:
            self.log.debug("Backup file %s matches expected md5 %s", 
                fqn, expected_hash)
            return True
            
        self.log.warn("Backup file %s does not match expected md5 "\
            "%s, got %s", fqn, expected_hash, key_md5 or key_etag)
        return False

    def iter_dir(self, relative_path, include_files=True, 
        include_dirs=False, recursive=False):
        
        key_name = relative_path
        if not key_name.endswith("/"):
            key_name = key_name + "/"
        fqn = self._fqn(key_name)
        
        self.log.debug("Starting to iterate the dir for %s:%s", 
            self.args.bucket_name, fqn)
        
        if include_files and not include_dirs and not recursive:
            # easier, we just want to list the keys. 
            return [
                key.name.replace(key_name, "")
                for key in self.bucket.list(prefix=fqn)
            ]
        
        items = []
        
        if not recursive:
            # return files and/or directories in this path
            for entry in self.bucket.list(prefix=fqn, delimiter="/"):
                if include_files and isinstance(entry, s3_key.Key):
                    items.append(entry.name.replace(key_name, ""))
                elif include_dirs:
                    items.append(entry.name.replace(key_name, ""))
            return items
        
        # recursive, we need to do a hierarchal list
        def _walk_keys(inner_key):
            for entry in self.bucket.list(prefix=inner_key, delimiter="/"):
                if isinstance(entry, s3_key.Key):
                    yield entry.name
                else:
                    # this is a directory
                    if include_dirs:
                        yield entry.name
                    for sub_entry in _walk_keys(entry.name):
                        yield sub_entry
        return list(_walk_keys(key_name))
    
    def remove_file(self, relative_path, dry_run=False):
        """Removes the file at the ``relative_path``. 
        
        Returns the full path to the file in the backup."""
        
        key_name = relative_path
        fqn = self._fqn(key_name)
        bucket_name = self.args.bucket_name
        
        if dry_run:
            return key_name
            
        self.log.debug("Starting to delete key %s:%s" % (
            self.args.bucket_name, key_name,))
        
        key = self.bucket.get_key(key_name)
        assert key is not None, "Cannot delete missing key %s:%s" % (
            self.args.bucket_name, key_name,)
            
        key.delete()

        self.log.debug("Finished deleting from %s:%s", self.args.bucket_name, 
            key_name)
        return key_name

    def remove_file_with_meta(self, relative_path, dry_run=False):
        """Removes the file at the ``relative_path`` that is expected to 
        have meta data. 
        
        Returns the fill path to the file in the backup."""
        
        # In S3 the meta is stored with the key. 
        return self.remove_file(relative_path, dry_run=dry_run)
        

    # ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    # Custom
    
    def _dict_to_aws_meta(self, data):
        """Turn a python dict into a dict suitable for use as S3 key meta. 
        
        All values must be strings.
        Does not support multi levels, so create a single level pipe delim.
        """
        
        
        def add_meta(meta, key, value, context=None):
            if key:
                if key.find("|") > -1:
                    raise ValueError("Key cannot contain a '|' char, got "\
                        "%s" % (key,))
                fq_key = "{context}{sep}{key}".format(context=context or "", 
                    sep="|" if context else "", key=key)
            else:
                fq_key = ""

            if isinstance(value, dict):
                for k, v in value.iteritems():
                    meta = add_meta(meta, k, v, context=fq_key)
                return meta
                
            elif isinstance(value, basestring):
                assert fq_key
                meta[fq_key] = value
                return meta
            else:
                raise ValueError("All values must be string or dict, got "\
                    "%s for %s" % (type(value), key))
    
        aws_meta = add_meta({}, None, data)
        self.log.debug("Converted data %s to aws_meta %s", data, aws_meta)
        return aws_meta
        
    def _aws_meta_to_dict(self, aws_meta):
        """Convert the aws meta to a multi level dict."""
        
        def set_value(data, key, value):
            head, _, tail = key.partition("|")
            if not tail:
                data[key] = value
                return
            # we have another level of dict
            data = data.setdefault(head, {})
            set_value(data, tail, value)
            return
            
        props = {}
        for k, v in aws_meta.iteritems():
            set_value(props, k, v)
        return props

    def _fqn(self, key_name):
        """Returns fully qualified name for the bucket and key.
        
        Note the fully qualified name is not a url. It has the form 
        <bucket_name>//key_path"""
        
        prefix = "%s/" % (self.args.key_prefix) if self.args.key_prefix\
            else "" 
        return  "%s%s" % (prefix, key_name)
        
    def _do_multi_part_upload(self, backup_file):
        
        fqn = self._fqn(backup_file.backup_path)
        self.log.debug("Starting multi part upload of %s to %s:%s", 
            backup_file, self.args.bucket_name, fqn)
        # All meta tags must be strings
        metadata = self._dict_to_aws_meta(backup_file.serialise())
        mp = self.bucket.initiate_multipart_upload(fqn, 
            metadata=metadata)
        
        timing = endpoints.TransferTiming(self.log, fqn, 
            backup_file.component.stat.size)
        chunk = None
        try:
            # Part numbers must start at 1
            for part, chunk in enumerate(self._chunk_file(
                backup_file.file_path),1):
                
                self.log.debug("Uploading part %s", part)
                try:
                    mp.upload_part_from_file(chunk, part, cb=timing.progress, 
                        num_cb=timing.num_callbacks)
                finally:
                    chunk.close()
        except (Exception):
            mp.cancel_upload()
            raise

        mp.complete_upload()
        self.log.debug("Finished multi part upload of %s to %s:%s", 
            backup_file, self.args.bucket_name, fqn)
        return fqn

    def _do_single_part_upload(self, backup_file):
        
        fqn = self._fqn(backup_file.backup_path)
        self.log.debug("Starting single part upload of %s to %s:%s", 
            backup_file, self.args.bucket_name, fqn)
        key = self.bucket.new_key(fqn)
        
        # All meta data fields have to be strings.
        key.update_metadata(self._dict_to_aws_meta(backup_file.serialise()))
        
        # # Rebuild the MD5 tuple boto makes
        # md5 = (
        #     backup_file.md5, 
        #     source_meta["md5_base64"], 
        #     source_meta["size"]
        # )
        timing = endpoints.TransferTiming(self.log, fqn, 
            backup_file.component.stat.size)
        key.set_contents_from_filename(backup_file.file_path, replace=False,
            cb=timing.progress, num_cb=timing.num_callbacks)

        self.log.debug("Finished single part upload of %s to %s:%s", 
            backup_file, self.args.bucket_name, fqn)
        return fqn
    
    def _chunk_file(self, file_path):
        """Yield chunks from ``file_path``.
        """
        
        chunk_bytes = self.args.multipart_chunk_size_mb * (1024**2)
        
        self.log.debug("Splitting file %s into chunks of %s bytes", 
            file_path, chunk_bytes)

        with open(file_path, 'rb') as f:
            chunk = f.read(chunk_bytes)
            while chunk:
                yield cStringIO.StringIO(chunk)
                chunk = f.read(chunk_bytes)
        return
