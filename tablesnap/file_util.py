"""Utilities for working with files."""

import datetime
import logging
import grp
import os
import os.path
import pwd
import re
import socket

import boto.utils

log = logging.getLogger(__name__)


def file_md5(file_path):

    fp = open(file_path, 'rb')
    try:
        # returns tuple (md5_hex, md5_base64, file_size)
        md5 = boto.utils.compute_md5(fp)
    finally:
        fp.close()
    return (md5[0], md5[1])

def file_meta(file_path):
    """Get a dict of the os file meta for this file. 
    """

    log.debug("Getting meta data for %(file_path)s" % vars())
    stat = os.stat(file_path)

    file_meta = {'uid': stat.st_uid,
        'gid': stat.st_gid,
        'mode': stat.st_mode,
        "size" : stat.st_size
    }

    try:
        file_meta['user'] = pwd.getpwuid(stat.st_uid).pw_name
    except (EnvironmentError):
        log.debug("Ignoring error getting user name.", exc_info=True)
        file_meta['user'] = ""

    try:
        file_meta['group'] = grp.getgrgid(stat.st_gid).gr_name
    except (EnvironmentError):
        log.debug("Ignoring error getting group name.", exc_info=True)
        file_meta['group'] = ""

    md5 = file_md5(file_path) 
    file_meta["md5_hex"] = md5[0]
    file_meta["md5_base64"] = md5[1]

    log.debug("For %(file_path)s got file meta %(file_meta)s " % vars())
    return file_meta

def ensure_dir(path):
    """Ensure the directories for ``path`` exist. 

    ``path`` may be a file path or directory path. 
    """

    if not os.path.isdir(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))
    return

