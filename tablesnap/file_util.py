"""Utilities for working with files."""

import datetime
import errno
import logging
import grp
import os
import os.path
import pwd
import re
import socket
import stat as stat_fn
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
    log.debug("Calculating md5 for %(file_path)s", 
        {"file_path" : file_path})
    start_ms = time.time() * 10**3
    fp = open(file_path, 'rb')
    try:
        # returns tuple (md5_hex, md5_base64, size)
        md5, _, _ = boto.utils.compute_md5(fp)
    finally:
        fp.close()
    duration_ms = (time.time() * 10**3) - start_ms
    log.debug("Calculated hash %(md5)s for %(file_path)s in %(ms)s ms", 
        {"md5":md5, "file_path":file_path, "ms":duration_ms})
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
    