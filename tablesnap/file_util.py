"""Utilities for working with Cassandra files."""

import datetime
import logging
import grp
import os
import os.path
import pwd
import re
import socket

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

def is_snapshot_path(file_path):

    head = os.path.dirname(file_path)
    if not head:
        raise ValueError("file_path does not include directory")
    while head != "/":
        head, tail = os.path.split(head)
        if tail == "snapshots":
            return True
    return False

# ============================================================================
# 

class Descriptor(object):
    """Implementation of o.a.c.io.sstable.Descriptor in the Cassandra 
    code base.

    Describes a single SSTable.

    Use :func:`from_file_path` to create instances.
    """

    def __init__(self, keyspace, cf_name, version, 
        generation, temporary):

        self.keyspace = keyspace
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
        
        keyspace = safe_pop()
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

        return (component, Descriptor(keyspace, cf_name, version, generation, 
            temporary))

# ============================================================================
#

class CassandraFile(object):
    """
    """

    @classmethod
    def from_file_path(cls, file_path, host=None, meta=None):

        component, descriptor = Descriptor.from_file_path(file_path)
        if meta is None:
            meta = file_meta(file_path)
        if host is None:
            host = socket.getfqdn()
        return CassandraFile(file_path, descriptor, component, host, meta)


    def __init__(self, file_path, descriptor, component, host, meta):

        self.file_path = file_path
        self.descriptor = descriptor
        self.component = component
        self.host = host
        self.meta = meta

    def __str__(self):
        return self.file_path

    @property
    def backup_path(self):
        """Gets the relative path to backup this file to. 
        """

        _, file_name = os.path.split(self.file_path)
        return os.path.join(*(
            "hosts",
            self.host,
            self.descriptor.keyspace,
            self.descriptor.cf_name,
            file_name,
        ))

# ============================================================================
#

class KeyspaceManifest(object):

    def __init__(self, keyspace, host, backup_name, timestamp, 
        column_families):
        
        self.keyspace = keyspace
        self.host = host
        self.backup_name = backup_name
        self.timestamp = timestamp
        self.column_families = column_families

    @classmethod
    def from_dir(cls, data_dir, keyspace):

        timestamp = datetime.datetime.now().isoformat()
        safe_ts = timestamp.replace(":", "_").replace(".", "_"
            ).replace("-", "_")
        backup_name = "%s-%s" % (safe_ts, socket.getfqdn())

        # Get a list of the files in each CF for this KS.
        # generate a list of (cf_name, cf_file_name)
        def gen_cf_files():
            # in cassandra 1.1 file layout is 
            # ks/cf/sstable-component

            ks_dir = os.path.join(data_dir, keyspace)
            _, cf_dirs, _ = os.walk(ks_dir).next()
            for cf_dir in cf_dirs:
                _, _, cf_files = os.walk(os.path.join(ks_dir, cf_dir)).next()
                for cf_file in cf_files:
                    yield(cf_dir, cf_file)

        column_families = {}
        for cf_name, file_name in gen_cf_files():
            try:
                _, desc = Descriptor.from_file_path(file_name)
            except (ValueError):
                # not a valid file name
                pass
            else:
                if not desc.temporary:
                    column_families.setdefault(cf_name, []).append(file_name)

        return cls(keyspace, socket.getfqdn(), backup_name, timestamp, 
            column_families)

    @classmethod
    def from_manifest(cls, manifest):

        return cls(manifest["keyspace"], manifest["host"], 
            manifest["name"], manifest["timestamp"], 
            manifest["column_families"])

    @classmethod
    def manifest_path(self, keyspace, backup_name):
        """
        """
        return os.path.join(*(
            "cluster",
            keyspace,
            "%s.json" % (backup_name,)
        ))

    @classmethod
    def keyspace_path(cls, keyspace):
        return os.path.join(*("cluster", keyspace))

    @classmethod
    def is_for_host(cls, manifest_file_name, host):

        name, _ = os.path.splitext(manifest_file_name)
        return name.endswith("-%s" % (host,))

    @property
    def path(self):
        """Gets the relative path to backup the keyspace manifest to."""

        # Would preferer to use the token here. 
        return KeyspaceManifest.manifest_path(self.keyspace, self.backup_name)

    def to_manifest(self):
        return {
            "host" : self.host,
            "keyspace" : self.keyspace,
            "timestamp" : self.timestamp,
            "name" : self.backup_name,
            "column_families" : self.column_families
        }

    def yield_file_names(self):
        """ 
        """

        for cf_name, cf_file_names in self.column_families.iteritems():
            for file_name in cf_file_names:
                yield (cf_name, file_name)


# Here be JUNK
def _file_index(file_path):

    dirname = os.path.dirname(file_path)
    return {
        dirname : os.listdir(dirname)
    }

