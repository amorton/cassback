"""Utilities for working with Cassandra files."""

import logging
import grp
import os
import os.path
import pwd
import re

import boto.utils

DATA_COMPONENT = "Data.db"
PRIMARY_INDEX_COMPONENT = "Index.db"
FILTER_COMPONENT = "Filter.db"
COMPRESSION_INFO_COMPONENT = "CompressionInfo.db"
STATS_COMPONENT = "Statistics.db"
DIGEST_COMPONENT = "Digest.sha1"
COMPACTED_MARKER = "Compacted"

TEMPORARY_MARKER = "tmp"

FILE_VERSION_PATTERN = re.compile("[a-z]+")

log = logging.getLogger(__name__)

class Descriptor(object):
    """Implementation of o.a.c.io.sstable.Descriptor in the Cassandra 
    code base.

    Describes a single SSTable.

    Use :func:`from_file_path` to create instances.
    """

    def __init__(self, ks_name, cf_name, version, 
        generation, temporary):

        self.ks_name = ks_name
        self.cf_name = cf_name
        self.major_version = version[0]
        self.minor_version = version[1] if len(version) > 1 else None
        self.generation = generation 
        self.temporary = temporary

    @classmethod
    def from_file_path(cls, file_path):
        """Parses ``file_path`` to create a :cls:`Descriptor`.

        `file_path` may be a full path or just a file name. Raises 
        :exc:`ValueError` if the ``file_path`` cannot be parsed.

        Returns a tuple of (component, descriptor).
        """
        _, file_name = os.path.split(file_path)

        tokens = file_name.split("-")
        def safe_pop():
            try:
                return tokens.pop(0)
            except (IndexError):
                raise ValueError("Not a valid SSTable file path "\
                    "%s" % (file_path,))
        def safe_peek():
            try:
                return tokens[0]
            except (IndexError):
                raise ValueError("Not a valid SSTable file path "\
                    "%s" % (file_path,))
        
        ks_name = safe_pop()
        cf_name = safe_pop()

        temporary = safe_peek() == TEMPORARY_MARKER
        if temporary:
            safe_pop()

        if FILE_VERSION_PATTERN.match(safe_peek()):
            version = safe_pop()
        else:
            # legacy
            version = "a"

        generation = int(safe_pop())
        component = safe_pop()

        return (component, Descriptor(ks_name, cf_name, version, generation, 
            temporary))

def is_live_file(file_path):
    try:
        _, desc = Descriptor.from_file_path(file_path)
        return desc.temporary == False
    except (ValueError):
        return False

def file_meta(file_path):
    """Get a dict of the os file meta. 
    """

    log.debug("Getting meta data for %(file_path)s" % vars())
    stat = os.stat(file_path)

    meta = {'uid': stat.st_uid,
        'gid': stat.st_gid,
        'mode': stat.st_mode,
        "size" : stat.st_size
    }

    try:
        meta['user'] = pwd.getpwuid(stat.st_uid).pw_name
    except (EnvironmentError):
        log.debug("Ignoring error getting user name.", exc_info=True)
        meta['user'] = ""

    try:
        meta['group'] = grp.getgrgid(stat.st_gid).gr_name
    except (EnvironmentError):
        log.debug("Ignoring error getting group name.", exc_info=True)
        meta['group'] = ""

    fp = open(file_path, 'rb')
    try:
        # returns tuple (md5_hex, md5_base64, file_size)
        meta["md5_hex"], meta["md5_base64"], boto_size = \
            boto.utils.compute_md5(fp)
    finally:
        fp.close()
    assert boto_size == stat.st_size, "File size is different."

    log.debug("For file %(file_path)s have meta %(meta)s " % vars())
    return meta

def _file_index(file_path):

    dirname = os.path.dirname(file_path)
    return {
        dirname : os.listdir(dirname)
    }

