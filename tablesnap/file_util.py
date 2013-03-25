"""Utilities for working with files."""

import datetime
import errno
import logging
import grp
import os
import os.path
import pwd
import re
import shutil
import socket
import stat as stat_fn
import tempfile
import time

import boto.utils

log = logging.getLogger(__name__)


def file_size(file_path):
    """Returns the byte size of a file at ``file_path``.
    """
    
    stat = os.stat(file_path)
    assert stat_fn.S_ISDIR(stat.st_mode) == False
    return stat.st_size

def file_md5(file_path):
    """Returns a string Hex md5 digest for the file at ``file_path``."""
    log.debug("Calculating md5 for %s", file_path)
    start_ms = time.time() * 10**3
    fp = open(file_path, 'rb')
    try:
        # returns tuple (md5_hex, md5_base64, size)
        md5, _, _ = boto.utils.compute_md5(fp)
    finally:
        fp.close()
    duration_ms = (time.time() * 10**3) - start_ms
    log.debug("Calculated hash %s for %s in %s ms", md5, file_path, 
        duration_ms)
    return md5

def ensure_dir(path):
    """Ensure the directories for ``path`` exist. 
    """

    
    try:
        os.makedirs(path)
    except (EnvironmentError) as e:
        if not(e.errno == errno.EEXIST and 
            e.filename == path):
            raise
    return

def maybe_remove_dirs(path):
    """Like :func:`os.removedirs` but ignores the error if the directory 
    is not empty.  
    """

    try:
        os.removedirs(path)
    except (EnvironmentError) as e:
        if e.errno != errno.ENOTEMPTY:
            raise
    return

def human_disk_bytes(bytes):
    """Format the ``bytes`` as a human readable value.
    """
    patterns = [
        (1024.0 ** 3, "G"),
        (1024.0 ** 2, "M"),
        (1024.0, "K")
    ]
    for scale, label in patterns:
        if bytes >= scale:
            return "{i:.1f}{label}".format(i=(bytes/scale), label=label)
    return "%sB" % (bytes,)


# ============================================================================
# Manages a stable hard link reference to a file

class FileReferenceContext(object):
    log = logging.getLogger("%s.%s" % (__name__, "FileReferenceContext"))
    
    def __init__(self, source_path):
        """Creates a temporary hard link for the file at 
        ``source_path``.
        
        The link is created when the context is entered and destroyed when 
        it is left. You can also call the ``link`` and ``close`` functions 
        to achieve the same result. 
        
        The full path of the ``source_path`` is re-created under a temporary 
        directory so that we can parse the path name for information.
        """
        
        self._source_path = source_path
        self._stable_dir = None
        self.stable_path = None
        self.ignore_next_exit = False
        
    def link(self):
        """Generates the stable link and returns it.
        
        If the link could not be generated because the source file was not 
        found ``None`` is returned. 
        
        Call ``close`` to delete the link."""
        
        self.__enter__()
        return self.stable_path
    
    def close(self):
        """Deletes the stable link."""
        
        self.__exit__(None, None, None)
        return
        
    def __enter__(self):
        """Enters the context and returns self if the link could be created. 
        
        If the link could not be created because the source path did not 
        exist ``None`` is returned.
        """
        if self.stable_path:
            return self.stable_path
        
        _, file_name = os.path.split(self._source_path)
        stable_dir = tempfile.mkdtemp(prefix="%s-" % file_name)
        assert self._source_path.startswith("/")
        stable_path = os.path.join(stable_dir, self._source_path[1:])
        
        self.log.debug("Linking %s to point to %s", stable_path, 
            self._source_path)
        ensure_dir(os.path.dirname(stable_path))
        try:
            os.link(self._source_path, stable_path)
        except (EnvironmentError) as e:
            if e.errno == errno.ENOENT:
                return None
            raise
        
        self._stable_dir = stable_dir
        self.stable_path = stable_path
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        
        if self.ignore_next_exit:
            self.ignore_next_exit = False
            return False
            
        if self._stable_dir:
            self.log.debug("Deleting temp dir for link %s", self.stable_path)
            shutil.rmtree(self._stable_dir)
            self._stable_dir = None
            self.stable_path = None 
        return False
        
