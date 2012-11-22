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

    
    try:
        os.makedirs(os.path.dirname(path))
    except (EnvironmentError) as e:
        if not(e.errno == errno.EEXIST and 
            e.filename == os.path.dirname(path)):
            raise
    return

def list_dirs(paths, fully_qualified=True, filter_hidden=True):
    """Lists the directories in a path. 

    ``paths`` is a list of paths to list from.

    if ``fully_qualified`` is specified the fully qualified directory path is
    returned, otherwise the local name is used. If ``filter_hidden`` hidden 
    files are not included in the result. 
    """
    return _list_entries(True, paths,fully_qualified=fully_qualified, 
        filter_hidden=filter_hidden)

def list_files(paths, fully_qualified=True, filter_hidden=True):
    return _list_entries(False, paths,fully_qualified=fully_qualified, 
        filter_hidden=filter_hidden)

def _list_entries(list_dirs, paths, fully_qualified=True, filter_hidden=True):

    if isinstance(paths, basestring):
        paths = [paths]

    entries = []
    for path in paths:
        try:
            if list_dirs:
                _, raw_items, _ = os.walk(path).next()
            else:
                _, _, raw_items = os.walk(path).next()
        except (StopIteration):
            # no items
            pass

        if filter_hidden:
            filtered = (
                raw_item
                for raw_item in raw_items
                if not raw_item.startswith(".")
            )
        else:
            filtered = raw_items

        if fully_qualified:
            entries.extend(
                os.path.join(path, raw_item)
                for raw_item in raw_items
            )
        else:
            entries.extend(raw_items)
    return entries

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



