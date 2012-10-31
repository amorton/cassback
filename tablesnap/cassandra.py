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

# _version = (1,1,0)

# def set_version(raw_version):

#     global _version
#     if isinstance(raw_version, basestring):
#         tokens = raw_version.split(".")
#     else:
#         tokens = raw_version[:3]
    
#     if not len(tokens) == 3:
#         raise ValueError("Not enough tokens in version %(raw_version)s" %\
#             vars())
#     try:
#         _version = tuple(
#             int(i)
#             for i in tokens
#         )
#     except (ValueError) as e:
#         raise ValueError("Non integer part in version %(raw_version)s" %\
#             vars())
#     log.info("Cassandra version set to %(_version)s" % vars())
#     return

# def get_version():
#     global _version
#     return _version

# def ver_has_per_cf_directory():
#     global _version
#     return _version >= (1,1,0)

def ver_has_per_cf_directory(vers):
    return vers >= (1,1,0)

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
        self.minor_version = version[1] if len(version) > 1 else ""
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
    @property
    def cass_version(self):
        """Returns the Cassandra version that created the current file by 
        inspecting the major and minor file version. 

        Cassandra version is returned as a three part integer tuple 
        (major, minor, rev).

        See o.a.c.io.sstable.Descriptor in the Cassandra code for up to date 
        info on the versions.

        At the time of writing::

            public static final String LEGACY_VERSION = "a"; // "pre-history"
            // b (0.7.0): added version to sstable filenames
            // c (0.7.0): bloom filter component computes hashes over raw key bytes instead of strings
            // d (0.7.0): row size in data component becomes a long instead of int
            // e (0.7.0): stores undecorated keys in data and index components
            // f (0.7.0): switched bloom filter implementations in data component
            // g (0.8): tracks flushed-at context in metadata component
            // h (1.0): tracks max client timestamp in metadata component
            // hb (1.0.3): records compression ration in metadata component
            // hc (1.0.4): records partitioner in metadata component
            // hd (1.0.10): includes row tombstones in maxtimestamp
            // he (1.1.3): includes ancestors generation in metadata component
            // hf (1.1.6): marker that replay position corresponds to 1.1.5+ millis-based id (see CASSANDRA-4782)
        """

        if self.major_version == "a":
            assert not self.minor_version
            return (0,6,0)

        if self.major_version >= "b" and self.major_version <= "f":
            assert not self.minor_version
            return (0,7,0)

        if self.major_version == "g":
            assert not self.minor_version
            return (0,8,0)

        if self.major_version == "h":
            if not self.minor_version:
                return (1,0,0)
            elif self.minor_version == "b":
                return (1,0,3)
            elif self.minor_version == "c":
                return (1,0,4)
            elif self.minor_version == "d":
                return (1,0,10)
            elif self.minor_version == "e":
                return (1,1,3)
            elif self.minor_version == "f":
                return (1,1,6)
        raise ValueError("Unknown file format "\
            "%(major_version)s%s(minor_version)s" % vars())


# ============================================================================
#

class CassandraFile(object):
    """
    """


    def __init__(self, descriptor, component, host, meta, original_path):
        self.descriptor = descriptor
        self.component = component
        self.host = host
        self.meta = meta
        self.original_path = original_path

    def __str__(self):
        return self.file_name

    @classmethod
    def from_file_path(cls, file_path, host=None, meta=None):

        component, descriptor = Descriptor.from_file_path(file_path)
        if meta is None:
            meta = file_util.file_meta(file_path)
        if host is None:
            host = socket.getfqdn()
        original_path = file_path if os.path.isfile(file_path) else None

        return CassandraFile(descriptor, component, host, meta, original_path)

    @property
    def file_name(self):
        return "%s-%s-%s%s-%s-%s" % (self.descriptor.keyspace, 
            self.descriptor.cf_name, 
            self.descriptor.major_version, 
            self.descriptor.minor_version, 
            self.descriptor.generation, 
            self.component
        )

    @property
    def backup_path(self):
        """Gets the relative path to backup this file to. 
        """

        return os.path.join(*(
            "hosts",
            self.host,
            self.descriptor.keyspace,
            self.descriptor.cf_name,
            self.file_name,
        ))

    @property
    def restore_path(self):
        """Gets the relative path to restore this file to. 
        """

        return os.path.join(*(
            self.descriptor.keyspace,
            self.descriptor.cf_name,
            self.file_name,
        ))

    @property
    def keyspace_dir(self):
        """Returns the keyspace directory for this path. 

        The object must have been created with an :attr:`orginal_path`.
        """

        assert self.original_path and os.path.isfile(self.original_path)

        if ver_has_per_cf_directory(self.descriptor.cass_version):
            # data/keyspace/cf/files.db
            ks_dir = os.path.abspath(os.path.join(os.path.dirname(
                self.original_path), os.path.pardir))
        else:
            # data/keyspace/files.db
            ks_dir = os.path.abspath(os.path.dirname(self.original_path))

        assert os.path.isdir(ks_dir)
        return ks_dir




# ============================================================================
#

class KeyspaceManifest(object):
    """A list of the CF files in a keyspace on a host. 
    """
    log = logging.getLogger("%s.%s" % (__name__, "KeyspaceManifest"))

    def __init__(self, keyspace, host, backup_name, timestamp, 
        column_families):
        
        self.keyspace = keyspace
        self.host = host
        self.backup_name = backup_name
        self.timestamp = timestamp
        self.column_families = column_families

    @classmethod
    def from_cass_file(cls, cass_file):
        """Create a manifest of the SSTables in a keyspace. 

        
        """

        timestamp = datetime.datetime.now()
        safe_ts = _to_safe_datetime_fmt(timestamp)
        timestamp = timestamp.isoformat()

        backup_name = "%s-%s-%s" % (safe_ts, cass_file.descriptor.keyspace, 
            socket.getfqdn())

        # Get a list of the files in each CF for this KS.
        # generate a list of (cf_name, cf_file_name)
        def gen_cf_files():

            ks_dir = cass_file.keyspace_dir
            if ver_has_per_cf_directory(cass_file.descriptor.cass_version):
                _, cf_dirs, _ = os.walk(ks_dir).next()
                for cf_dir in cf_dirs:
                    _, _, cf_files = os.walk(os.path.join(ks_dir, cf_dir
                        )).next()
                    for cf_file in cf_files:
                        yield os.path.join(cf_dir, cf_file)
            else:
                _, _, cf_files = os.walk(ks_dir).next()
                for cf_file in cf_files:
                    yield os.path.join(ks_dir, cf_file)

        column_families = {}
        for cf_file_path in gen_cf_files():
            try:
                _, desc = Descriptor.from_file_path(cf_file_path)
            except (ValueError):
                # not a valid file name
                pass
            else:
                if not desc.temporary:
                    _, file_name = os.path.split(cf_file_path)
                    column_families.setdefault(desc.cf_name, []
                        ).append(file_name)

        return cls(cass_file.descriptor.keyspace, socket.getfqdn(), 
            backup_name, timestamp, column_families)

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


    @classmethod
    def backup_dir(cls, keyspace):
        return os.path.join(*("cluster", keyspace))

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
                yield file_name
