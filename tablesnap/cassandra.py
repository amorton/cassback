"""Utilities for working with Cassandra and the versions."""

import datetime
import logging
import grp
import os
import os.path
import pwd
import re
import socket

import file_util

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

# ============================================================================
# utility. 

_SAFE_DT_FMT = "%Y_%m_%dT%H_%M_%S_%f"

def _to_safe_datetime_fmt(dt):
    """Convert the datetime ``dt`` instance to a file system safe format. 
    """

    return dt.strftime(_SAFE_DT_FMT)

def _from_safe_datetime_fmt(dt_str):
    return datetime.datetime.strptime(dt_str, _SAFE_DT_FMT)


# ============================================================================
# Version / Feature detection

_version = (1,1,0)

def set_version(raw_version):

    global _version
    if isinstance(raw_version, basestring):
        tokens = raw_version.split(".")
    else:
        tokens = raw_version[:3]
    
    if not len(tokens) == 3:
        raise ValueError("Not enough tokens in version %(raw_version)s" %\
            vars())
    try:
        _version = tuple(
            int(i)
            for i in tokens
        )
    except (ValueError) as e:
        raise ValueError("Non integer part in version %(raw_version)s" %\
            vars())
    log.info("Cassandra version set to %(_version)s" % vars())
    return

def get_version():
    global _version
    return _version

def ver_has_per_cf_directory():
    global _version
    return _version >= (1,1,0)

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
# SSTable Descriptor

class Descriptor(object):
    """Implementation of o.a.c.io.sstable.Descriptor in the Cassandra 
    code base.

    Describes a single SSTable.

    Use :func:`from_file_path` to create instances.
    """

    def __init__(self, keyspace, cf_name, version, generation, temporary):

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


    def __init__(self, file_path, descriptor, component, host, meta):

        self.file_path = file_path
        self.descriptor = descriptor
        self.component = component
        self.host = host
        self.meta = meta


    def __str__(self):
        return self.file_path

    @classmethod
    def from_file_path(cls, file_path, host=None, meta=None):

        component, descriptor = Descriptor.from_file_path(file_path)
        if meta is None:
            meta = file_util.file_meta(file_path)
        if host is None:
            host = socket.getfqdn()
        return CassandraFile(file_path, descriptor, component, host, meta)

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
    """A list of the CF files in a keyspace on a host. 
    """

    def __init__(self, keyspace, host, backup_name, timestamp, 
        column_families):
        
        self.keyspace = keyspace
        self.host = host
        self.backup_name = backup_name
        self.timestamp = timestamp
        self.column_families = column_families

    @classmethod
    def from_dir(cls, data_dir, keyspace):

        timestamp = datetime.datetime.now()
        safe_ts = _to_safe_datetime_fmt(timestamp)
        timestamp = timestamp.isoformat()

        backup_name = "%s-%s-%s" % (safe_ts, keyspace, socket.getfqdn())

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
    def from_backup_name(cls, backup_name):

        #only get 2 splits, the host name may have "-"
        tokens = backup_name.split("-", 2)
        
        safe_ts = tokens.pop(0)
        keyspace = tokens.pop(0)
        host_name = tokens.pop(0)
        assert not tokens

        # expecting 2012_10_22T14_26_57_871835 for the safe TS. 
        timestamp = _from_safe_datetime_fmt(safe_ts).isoformat()
        return cls(keyspace, host_name, backup_name, timestamp, None)

    @classmethod
    def from_manifest(cls, manifest):

        return cls(manifest["keyspace"], manifest["host"], 
            manifest["name"], manifest["timestamp"], 
            manifest["column_families"])

    # @classmethod
    # def manifest_path(self, keyspace, backup_name):
    #     """
    #     """
    #     return os.path.join(*(
    #         "cluster",
    #         keyspace,
    #         "%s.json" % (backup_name,)
    #     ))

    @classmethod
    def backup_dir(cls, keyspace):
        return os.path.join(*("cluster", keyspace))

    # @classmethod
    # def is_for_host(cls, manifest_file_name, host):

    #     name, _ = os.path.splitext(manifest_file_name)
    #     return name.endswith("-%s" % (host,))

    @property
    def backup_path(self):
        """Gets the relative path to backup the keyspace manifest to."""

        return os.path.join(*(
            "cluster",
            self.keyspace,
            "%s.json" % (self.backup_name,)
        ))

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

        assert self.column_families is not None

        for cf_name, cf_file_names in self.column_families.iteritems():
            for file_name in cf_file_names:
                yield (cf_name, file_name)
