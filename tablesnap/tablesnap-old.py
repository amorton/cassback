#!/usr/bin/env python
import pyinotify
import boto

from optparse import OptionParser
from traceback import format_exc
from threading import Thread
from Queue import Queue
import logging
import os.path
import socket
import json
import sys
import os
import pwd
import grp
import signal
import StringIO

default_log = logging.getLogger('tablesnap')
stderr = logging.StreamHandler()
stderr.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
default_log.addHandler(stderr)
if os.environ.get('TDEBUG', False):
    default_log.setLevel(logging.DEBUG)
else:
    default_log.setLevel(logging.INFO)

# Default number of writer threads
default_threads = 4

# Default retries
default_retries = 1

# S3 limit for single file upload
s3_limit = 5 * 2**30

# Max file size to upload without doing multipart in MB
max_file_size = 5120

# Default chunk size for multipart uploads in MB
default_chunk_size = 256

class S3Config(object):

    aws_key = None
    """AWS API Key"""

    aws_secret = None
    """AWS Secret Key"""

    bucket_name = None
    """S3 Bucket name"""

    prefix = ""
    """Prefix for uploaded files."""

    host_name = socket.getfqdn()
    """Host name to use when uploading. Defaults to ``socket.getfqdn()``"""

    max_file_size_mb = 5120
    """Max file to upload without multipart uploads. Default is 5120"""

    chunk_size_mb= 256
    """Chunk size for multipart uploads. Default is 256"""

    def __init__(self, key=None, aws_secret=None, bucket_name=None,
        prefix=None, name=None, max_file_size_mb=None, chunk_size_mb=None):

        self.aws_key = key
        self.aws_secret = aws_secret
        self.bucket_name = bucket_name
        self.prefix = prefix or self.prefix
        self.name = name or self.name
        self.max_file_size_mb = max_file_size_mb or self.max_file_size_mb 
        self.chunk_size_mb = chunk_size_mb or self.chunk_size_mb

    @property
    def max_file_size_bytes(self):
        return self.max_file_size_mb * 2**20

    @property
    def chunk_size_bytes(self):
        return self.chunk_size_mb * 2 ** 20

    def clone(self):
        return UploadConfig(**vars(self))

class S3UploadWorker(Thread):

    def __init__(self, thread_name, config, log=default_log):
        Thread.__init__(self, name=thread_name)

        self.daemon = True
        self.fileq = fileq
        self.config = config
        self.log = log

    def run(self):

        bucket = self.get_bucket()

        while True:
            f = self.fileq.get()
            keyname = self.build_keyname(f)
            try:
                self.upload_sstable(bucket, keyname, f)
            except:
                self.log.critical("Failed uploading %s. Aborting.\n%s" %
                             (f, format_exc()))
                # Brute force kill self
                os.kill(os.getpid(), signal.SIGKILL)

            self.fileq.task_done()
        return

    def build_keyname(self, pathname):
        return '%s%s:%s' % (self.config.prefix or '', self.config.name, 
            pathname)

    def get_bucket(self):
        # Reconnect to S3
        s3 = boto.connect_s3(self.config.aws_key, self.config.aws_secret)
        return s3.get_bucket(self.bucket_name)

    #
    # Check if this keyname (ie, file) has already been uploaded to
    # the S3 bucket. This will verify that not only does the keyname
    # exist, but that the MD5 sum is the same -- this protects against
    # partial or corrupt uploads.
    #
    def key_exists(self, bucket, keyname, filename, stat):
        key = None
        for r in range(self.retries):
            try:
                key = bucket.get_key(keyname)
                if key == None:
                    self.log.debug('Key %s does not exist' % (keyname,))
                    return False
                else:
                    self.log.debug('Found key %s' % (keyname,))
                    break
            except:
                bucket = self.get_bucket()
                continue

        if key == None:
            self.log.critical("Failed to lookup keyname %s after %d"
                              " retries\n%s" %
                             (keyname, self.retries, format_exc()))
            raise

        if key.size != stat.st_size:
            self.log.warning('ATTENTION: your source (%s) and target (%s) '
                'sizes differ, you should take a look. As immutable files '
                'never change, one must assume the local file got corrupted '
                'and the right version is the one in S3. Will skip this file '
                'to avoid future complications' % (filename, keyname, ))
            return True
        else:
            # Compute MD5 sum of file
            try:
                fp = open(filename, "r")
            except IOError as (errno, strerror):
                if errno == 2:
                    # The file was removed, return True to skip this file.
                    return True

                self.log.critical("Failed to open file: %s (%s)\n%s" %
                             (filename, strerror, format_exc(),))
                raise

            md5 = key.compute_md5(fp)
            fp.close()
            self.log.debug('Computed md5: %s' % (md5,))

            meta = key.get_metadata('md5sum')

            if meta:
                self.log.debug('MD5 metadata comparison: %s == %s? : %s' %
                              (md5[0], meta, (md5[0] == meta)))
                result = (md5[0] == meta)
            else:
                self.log.debug('ETag comparison: %s == %s? : %s' %
                              (md5[0], key.etag.strip('"'),
                              (md5[0] == key.etag.strip('"'))))
                result = (md5[0] == key.etag.strip('"'))
                if result:
                    self.log.debug('Setting missing md5sum metadata for %s' %
                                  (keyname,))
                    key.set_metadata('md5sum', md5[0])
        
        if result:
            self.log.info("Keyname %s already exists, skipping upload"
                          % (keyname))
        else:
            self.log.warning('ATTENTION: your source (%s) and target (%s) '
                'MD5 hashes differ, you should take a look. As immutable '
                'files never change, one must assume the local file got '
                'corrupted and the right version is the one in S3. Will '
                'skip this file to avoid future complications' % 
                (filename, keyname, ))
            return True

        return result

    def get_free_memory_in_kb(self):
        f = open('/proc/meminfo', 'r')
        memlines = f.readlines()
        f.close()
        lines = []
        for line in memlines:
            ml = line.rstrip(' kB\n').split(':')
            lines.append((ml[0], int(ml[1].strip())))
        d = dict(lines)
        return d['Cached'] + d['MemFree'] + d['Buffers']

    def split_sstable(self, filename):
        free = self.get_free_memory_in_kb() * 1024
        self.log.debug('Free memory check: %d < %d ? : %s' %
            (free, self.chunk_size, (free < self.chunk_size)))
        if free < self.chunk_size:
            self.log.warn('Your system is low on memory, '
                          'reading in smaller chunks')
            chunk_size = free / 20
        else:
            chunk_size = self.chunk_size
        self.log.debug('Reading %s in %d byte sized chunks' %
                       (filename, chunk_size))
        f = open(filename, 'rb')
        while True:
            chunk = f.read(chunk_size)
            if chunk:
                yield StringIO.StringIO(chunk)
            else:
                break
        if f and not f.closed:
            f.close()

    def upload_sstable(self, bucket, keyname, filename, with_index=True):

        self.log.debug('Uploading file %s to key %s' % (filename, keyname,))

        # Include the file system metadata so that we have the
        # option of using it to restore the file modes correctly.
        #
        try:
            stat = os.stat(filename)
        except OSError:
            # File removed?
            self.log.warn("Could not stat file %s, skipping" % (filename,),  
                exc_info=sys.exc_info())
            return

        if self.key_exists(bucket, keyname, filename, stat):
            self.log.warn("S3 Key %s for file %s, exists skipping" % (
                keyname, filename,))
            return
        
        fp = open(filename, 'rb')
        try:
            md5 = boto.utils.compute_md5(fp)
        finally:
            fp.close()
        self.log.debug('Computed md5sum before upload is: %s' % (md5,))
        

        def progress(sent, total):
            if sent == total:
                self.log.info('Finished uploading %s' % filename)

        
        if with_index:
            dirname = os.path.dirname(filename)
            json_str = json.dumps({dirname: os.listdir(dirname)})
            
            for r in range(self.config.retries):
                try:
                    key = bucket.new_key('%s-listdir.json' % keyname)
                    key.set_contents_from_string(json_str,
                        headers={'Content-Type': 'application/json'},
                        replace=True)
                    break
                except (Exception):
                    if r == self.config.retries - 1:
                        self.log.critical("Failed to upload directory "\
                            "listing.", exc_info=sys.exc_info)
                        raise
                    else:
                        self.log.info("Error uploading directory, attempt "\
                            "%s of %s" % (r+1, self.config.retries), 
                            exc_info=sys.exc_info())
                    bucket = self.get_bucket()

        meta = {'uid': stat.st_uid,
                'gid': stat.st_gid,
                'mode': stat.st_mode}
        try:
            u = pwd.getpwuid(stat.st_uid)
            meta['user'] = u.pw_name
        except (Exception):
            pass

        try:
            g = grp.getgrgid(stat.st_gid)
            meta['group'] = g.gr_name
        except (Exception):
            pass
        meta_json = json.dumps(meta)

        self.log.info('Uploading %s' % filename)

        for r in range(self.config.retries):
            try:
                self.log.debug('File size check: %s > %s ? : %s' %
                    (stat.st_size, self.max_size,
                    (stat.st_size > self.max_size),))

                if stat.st_size > self.max_size:
                    self.log.info('Performing multipart upload for %s' %
                                 (filename))

                    mp = bucket.initiate_multipart_upload(keyname,
                        metadata={'stat': meta_json, 'md5sum': md5[0]})
                    part = 1
                    chunk = None
                    try:
                        for chunk in self.split_sstable(filename):
                            self.log.debug('Uploading part #%d '
                                           '(size: %d)' %
                                           (part, chunk.len,))
                            mp.upload_part_from_file(chunk, part)
                            chunk.close()
                            part += 1
                    except (Exception):
                        self.log.info('Error uploading part %d attempt '
                            '%s of %s' % (part, r+1, self.config.retries), 
                            exc_info=sys.exc_info())
                        mp.cancel_upload()
                        if chunk:
                            chunk.close()
                        raise
                    self.log.debug('Uploaded %d parts, '
                                   'completing upload' % (part,))
                    mp.complete_upload()
                    progress(100, 100)

                else:
                    self.log.debug('Performing monolithic upload')
                    key = bucket.new_key(keyname)
                    key.set_metadata('stat', meta_json)
                    key.set_metadata('md5sum', md5[0])
                    key.set_contents_from_filename(filename, replace=True,
                                                   cb=progress, num_cb=1,
                                                   md5=md5)
                break
            except (Exception):
                if not os.path.exists(filename):
                    self.log.warn("File %s was removed while uploading" % (
                        filename,))
                    return

                if r == self.config.retries - 1:
                    self.log.critical("Failed to upload directory "\
                        "listing.", exc_info=sys.exc_info)
                    raise
                else:
                    self.log.info("Error uploading directory, attempt "\
                        "%s of %s" % (r+1, self.config.retries), 
                        exc_info=sys.exc_info())
                bucket = self.get_bucket()
        return



class UploadHandler(pyinotify.ProcessEvent):
    def my_init(self, threads=None, key=None, secret=None, bucket_name=None,
                prefix=None, name=None, max_size=None, chunk_size=None,
                log=default_log):
        # self.key = key
        # self.secret = secret
        # self.bucket_name = bucket_name
        # self.prefix = prefix
        # self.name = name or socket.getfqdn()
        self.retries = default_retries
        self.log = log

        # if max_size:
        #     self.max_size = max_size * 2**20
        # else:
        #     self.max_size = max_file_size * 2**20

        # if chunk_size:
        #     self.chunk_size = chunk_size * 2**20
        # else:
        #     self.chunk_size = None

        self.fileq = Queue()
        for i in range(int(threads)):
            t = UploadWorker("worker-%s" % i, fileq, 
                key, secret, bucket_name, prefix=prefix, name=name, 
                max_size=max_size, chunk_size=chunk_size, log=log)
            t.start()

    def add_file(self, filename):
        if filename.find('-tmp') == -1:
            self.fileq.put(filename)

    def process_IN_MOVED_TO(self, event):
        self.add_file(event.pathname)

class WatchdogHandler(watchdog.FileSystemEventHandler):

    def __init__
 
def backup_file(handler, filename, filedir):
    if filename.find('-tmp') != -1:
        return

def backup_files(handler, paths, log=default_log):
    for path in paths:
        log.info('Backing up %s' % path)
        for filename in os.listdir(path):
            if filename.find('-tmp') != -1:
                continue
    handler.add_file(fullpath)

def backup_files(handler, paths, recurse):
    for path in paths:
        log.info('Backing up %s' % path)
        if recurse:
            for root, dirs, files in os.walk(path):
                for filename in files:
                    backup_file(handler, filename, root)
        else:
            for filename in os.listdir(path):
                backup_file(handler, filename, path)
    return 0


def main():
    parser = OptionParser(usage='%prog [options] <bucket> <path> [...]')
    parser.add_option('-k', '--aws-key', dest='aws_key', default=None)
    parser.add_option('-s', '--aws-secret', dest='aws_secret', default=None)
    parser.add_option('-r', '--recursive', action='store_true', dest='recursive', default=False,
        help='Recursively watch the given path(s)s for new SSTables')
    parser.add_option('-a', '--auto-add', action='store_true', dest='auto_add', default=False,
        help='Automatically start watching new subdirectories within path(s)')
    parser.add_option('-p', '--prefix', dest='prefix', default=None,
        help='Set a string prefix for uploaded files in S3')
    parser.add_option('-t', '--threads', dest='threads', default=default_threads,
                      help='Number of writer threads')
    parser.add_option('-n', '--name', dest='name', default=None,
        help='Use this name instead of the FQDN to identify the SSTables from this host')
    parser.add_option('--max-upload-size', dest='max_upload_size',
        default=max_file_size,
        help='Max size for files to be uploaded before doing multipart '
        '(default %dM)' % max_file_size)
    parser.add_option('--multipart-chunk-size', dest='multipart_chunk_size',
        default=default_chunk_size,
        help='Chunk size for multipart uploads (default: %dM or 10%% of '
        'free memory if default is not available)' %
        (default_chunk_size,))
    options, args = parser.parse_args()

    if len(args) < 2:
        parser.print_help()
        return -1

    bucket = args[0]
    paths = args[1:]

    # Check S3 credentials only. We reconnect per-thread to avoid any
    # potential thread-safety problems.
    s3 = boto.connect_s3(options.aws_key, options.aws_secret)
    bucket = s3.get_bucket(bucket)

    handler = UploadHandler(threads=options.threads, key=options.aws_key,
                            secret=options.aws_secret, bucket_name=bucket,
                            prefix=options.prefix, name=options.name,
                            max_size=int(options.max_upload_size),
                            chunk_size=int(options.multipart_chunk_size))

    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm, handler)
    for path in paths:
        ret = wm.add_watch(path, pyinotify.IN_MOVED_TO, rec=options.recursive,
                           auto_add=options.auto_add)
        if ret[path] == -1:
            default_log.critical('add_watch failed for %s, bailing out!' %
                                (path))
            return 1

    backup_files(handler, paths, options.recursive)

    notifier.loop()

if __name__ == '__main__':
    sys.exit(main())
