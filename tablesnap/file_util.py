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

class CassandraFile(object):
    """
    """

    def __init__(self, file_path, descriptor, component):

        assert os.path.dirname(file_path), "file_path does not inlcude dir"
        self.file_path = file_path
        self.descriptor = descriptor
        self.component = component
        self._file_meta = None

    def __str__(self):
        return self.file_path

    @classmethod
    def from_file_path(cls, file_path):

        component, descriptor = Descriptor.from_file_path(file_path)
        return CassandraFile(file_path, descriptor, component)

    def in_snapshot(self):
        """Returns ``True`` if this file is in a snapshot."""

        head = os.path.dirname(self.file_path)
        while head != "/":
            head, tail = os.path.split(head)
            if tail == "snapshots":
                return True
        return False

    def should_backup(self):
        """Returns ``True`` if this file should be backed up. 
        """
        return (self.descriptor.temporary == False) and \
            (self.in_snapshot() == False)

    @property
    def file_meta(self):
        """Get a dict of the os file meta for this file. 
        """

        if self._file_meta is not None:
            return self._file_meta

        log.debug("Getting meta data for %(file_path)s" % vars(self))
        stat = os.stat(self.file_path)

        self._file_meta = {'uid': stat.st_uid,
            'gid': stat.st_gid,
            'mode': stat.st_mode,
            "size" : stat.st_size
        }

        try:
            self._file_meta['user'] = pwd.getpwuid(stat.st_uid).pw_name
        except (EnvironmentError):
            log.debug("Ignoring error getting user name.", exc_info=True)
            self._file_meta['user'] = ""

        try:
            self._file_meta['group'] = grp.getgrgid(stat.st_gid).gr_name
        except (EnvironmentError):
            log.debug("Ignoring error getting group name.", exc_info=True)
            self._file_meta['group'] = ""

        fp = open(self.file_path, 'rb')
        try:
            # returns tuple (md5_hex, md5_base64, file_size)
            md5 = boto.utils.compute_md5(fp)
        finally:
            fp.close()
        self._file_meta["md5_hex"] = md5[0]
        self._file_meta["md5_base64"] = md5[1]
        assert md5[2] == stat.st_size, "File size is different."

        log.debug("Got file meta %(_file_meta)s " % vars(self))
        return self._file_meta

    def backup_path(self, host=None):
        """Gets the relative path to backup this file to. 
        """

        _, file_name = os.path.split(self.file_path)
        return os.path.join(*(
            "hosts",
            host or socket.getfqdn(),
            self.descriptor.ks_name,
            self.descriptor.cf_name,
            file_name,
        ))

class KeyspaceManifest(object):

    def __init__(self, data_dir, ks_name):
        self.data_dir = data_dir
        self.ks_name = ks_name
        self.timestamp = datetime.datetime.now().isoformat()

        # Get a list of the files in each CF for this KS.
        # generate a list of (cf_name, cf_file_name)

        def gen_cf_files():
            # in cassandra 1.1 file layout is 
            # ks/cf/sstable-component

            ks_dir = os.path.join(data_dir, ks_name)
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

        self.manifest = {
            "host" : socket.getfqdn(),
            "keyspace" : self.ks_name,
            "timestamp" : self.timestamp,
            "column_families" : column_families
        }


    def backup_path(self):
        """Gets the relative path to backup the keyspace manifest to."""

        # Would preferer to use the token here. 

        safe_ts = self.timestamp.replace(":", "_").replace(".", "_")
        file_name = "%s-%s.json" % (safe_ts, socket.getfqdn())

        return os.path.join(*(
            "cluster",
            self.ks_name,
            file_name
            ))


# Here be JUNK
def _file_index(file_path):

    dirname = os.path.dirname(file_path)
    return {
        dirname : os.listdir(dirname)
    }

