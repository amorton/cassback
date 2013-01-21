#!/usr/bin/env python
# encoding: utf-8

# Copyright 2012 Aaron Morton
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities for working with Cassandra and the versions."""

import datetime
import logging
import grp
import os
import os.path
import pwd
import re
import socket

import dt_util, file_util

class Components(object):
    """Constants for Cassandra SSTable components."""

    DATA = "Data.db"
    PRIMARY_INDEX = "Index.db"
    FILTER = "Filter.db"
    COMPRESSION_INFO = "CompressionInfo.db"
    STATS = "Statistics.db"
    DIGEST = "Digest.sha1"

COMPACTED_MARKER = "Compacted"
"""Marker added to compacted files."""

TEMPORARY_MARKER = "tmp"
"""Marker used to identify temp sstables that are being created."""

FILE_VERSION_PATTERN = re.compile("[a-z]+")

log = logging.getLogger(__name__)

# ============================================================================
#  

MIN_VERSION = (1,0,0)

TARGET_VERSION = None
"""Cassandra version we are working with. Used for file paths and things.

Cannot use the version of the file because when 1.1 starts it moves files to 
new locations but does not change their file version. 
"""

def set_version(ver):
    """Set the global cassandra version. 
    
    If ``ver`` is string we expect the form "major.minor.rev". Otherwise it is 
    expected to be a tuple of ints. 
    """
    
    global TARGET_VERSION
    if isinstance(ver, basestring):
        TARGET_VERSION = tuple(int(i) for i in ver.split(".")[:3])  
    else:
        TARGET_VERSION = ver
    log.info("Cassandra version changed to {ver}".format(ver=TARGET_VERSION))
    return
    
# ============================================================================
# Utility. 

_SAFE_DT_FMT = "%Y_%m_%dT%H_%M_%S_%f"
"""strftime() format to safely use a datetime in file name."""

def _to_safe_datetime_fmt(dt):
    """Convert the datetime ``dt`` instance to a file system safe format. 
    """
    return dt.strftime(_SAFE_DT_FMT)

def _from_safe_datetime_fmt(dt_str):
    """Convert the string ``dt_str`` from a file system safe format."""
    return datetime.datetime.strptime(dt_str, _SAFE_DT_FMT)

def is_snapshot_path(file_path):
    """Returns true if this path is a snapshot path. 
    
    It's a pretty simple test: does it have 'snapshots' in it.
    """
    head = os.path.dirname(file_path or "")
    if not head:
        raise ValueError("file_path {file_path} does not include "\
            "directory".format(file_path=file_path))
    while head != "/":
        head, tail = os.path.split(head)
        if tail == "snapshots":
            return True
    return False


# ============================================================================
# SSTable Descriptor

class FileStat(object):
    """Basic file stats"""
    log = logging.getLogger("%s.%s" % (__name__, "FileStat"))
    
    def __init__(self, file_path, uid=None, user=None, gid=None, group=None,
        mode=None, size=None):
        
        def meta():
            try:
                return meta.data
            except (AttributeError):
                meta.data = self._extract_meta(file_path)
            return meta.data
            
        self.file_path = file_path
        self.uid = uid or meta()["uid"]
        self.gid = gid or meta()["gid"]
        self.mode = mode or meta()["mode"]
        self.size = size or meta()["size"]
        self.user = user or meta()["user"]
        self.group = group or meta()["group"]
    
    def __str__(self):
        return "FileStat for {file_path}: uid {uid}, user {user}, gid {gid},"\
            " group {group}, mode {mode}, size {size}".format(**vars(self))
    
    def serialise(self):
        """Serialise the state to a dict."""
        return dict(vars(self))
    
    @classmethod
    def deserialise(cls, data):
        """Create an instance use the ``data`` dict."""
        assert data        
        return cls(data["file_path"], uid=data["uid"], user=data["user"], 
            gid=data["gid"], group=data["group"], mode=data["mode"], 
            size=data["size"])
            
    def _extract_meta(self, file_path):
        """Get a dict of the os file meta for the ``file_path``
        
        Allow OS errors to bubble out as files can be removed during 
        processing.
        """

        stat = os.stat(file_path)
        file_meta = {
            "uid" : stat.st_uid,
            "gid" : stat.st_gid,
            "mode" : stat.st_mode,
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

        log.debug("For {file_path} got meta {file_meta} ".format(
            file_path=file_path, file_meta=file_meta))
        return file_meta
    
class SSTableComponent(object):
    """Meta data about a component file for an SSTable.
    
    e.g. the -Data.db file.
    """
    log = logging.getLogger("%s.%s" % (__name__, "SSTableComponent"))
    
    def __init__(self, file_path, keyspace=None, cf=None, version=None, 
        generation=None, component=None, temporary=None, stat=None):
        
        def props(): 
            try:
                return props.data
            except (AttributeError):
                props.data = self._component_properties(file_path)
            return props.data
        
        self.file_path = file_path
        self.keyspace = keyspace or props()["keyspace"]
        self.cf = cf or props()["cf"]
        self.version = version or props()["version"]
        self.generation = generation or props()["generation"]
        self.component = component or props()["component"]
        self.temporary = temporary or props()["temporary"]
        self.stat = stat or FileStat(file_path)
    
    def __str__(self):
        return "SSTableComponent for {file_path}: keyspace {keyspace}, "\
            "cf {cf}, version {version}, generation {generation}, "\
            "component {component}, temporary {temporary}, "\
            "{stat}".format(**vars(self))

    def serialise(self):
        """Serialise the state to a dict."""
        return {
            "file_path" : self.file_path, 
            "keyspace" : self.keyspace, 
            "cf" : self.cf,
            "version" : self.version, 
            "generation" : self.generation,
            "component" : self.component, 
            "temporary" : self.temporary, 
            "stat" : self.stat.serialise()
        }

    
    @classmethod
    def deserialise(cls, data):
        """Create an instance use the ``data`` dict."""
        assert data
        return cls(data["file_path"], keyspace=data["keyspace"], 
            cf=data["cf"], version=data["version"], 
            generation=data["generation"], component=data["component"], 
            temporary=data["temporary"], 
            stat=FileStat.deserialise(data["stat"]))
            
    def _component_properties(self, file_path):
        """Parses ``file_path`` to extact the component tokens.

        Raises :exc:`ValueError` if the ``file_path`` cannot be parsed.
        
        Returns a dict of the component properties.
        """
        self.log.debug("Parsing file path {file_path}".format(
            file_path=file_path))
            
        _, file_name = os.path.split(file_path)
        tokens = file_name.split("-")
        def pop():
            """Pop from the tokens. 
            Expected a token to be there.
            """
            try:
                return tokens.pop(0)
            except (IndexError):
                raise ValueError("Not a valid SSTable file path "\
                    "{file_path}".format(file_path=file_path))
        def peek():
            """Peeks the tokens. 
            Expected a token to be there.
            """
            try:
                return tokens[0]
            except (IndexError):
                raise ValueError("Not a valid SSTable file path "\
                    "{file_path}".format(file_path=file_path))
        
        properties = {
            "keyspace" :  pop() if TARGET_VERSION >= (1,1,0) else None,
            "cf" : pop(),
            "temporary" : peek() == TEMPORARY_MARKER
        }
        
        if properties["temporary"]:
            pop()
        
        #Older versions did not use two character file versions.
        if FILE_VERSION_PATTERN.match(peek()):
            properties["version"] = pop()
        else:
            # legacy
            properties["version"] = "a"

        properties["generation"] = int(pop())
        properties["component"] = pop()

        return properties
    
    @property
    def file_name(self):
        """Returns the file name for the componet formatted to the 
        current `TARGET_VERSION`.
        """
        
        if TARGET_VERSION < (1,1,0):
            # pre 1.1 the file name was CF-version-generation-component
            fmt = "{cf}-{version}-{generation}-{component}"
        else:
            # Assume 1.1 and beyond 
            # file name adds the keyspace. 
            fmt = "{keyspace}-{cf}-{version}-{generation}-{component}"
        return fmt.format(**vars(self))
        
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
            // ia (1.2.0): column indexes are promoted to the index file
            //             records estimated histogram of deletion times in tombstones
            //             bloom filter (keys and columns) upgraded to Murmur3
            // ib (1.2.1): tracks min client timestamp in metadata component
        """
        
        major_version = self.version[0]
        minor_version = self.version[1] if len(self.version) > 1 else ""
        
        if major_version == "a":
            assert not minor_version
            return (0,6,0)

        if major_version >= "b" and major_version <= "f":
            assert not minor_version
            return (0,7,0)

        if major_version == "g":
            assert not minor_version
            return (0,8,0)

        if major_version == "h":
            if not minor_version:
                return (1,0,0)
            elif minor_version == "b":
                return (1,0,3)
            elif minor_version == "c":
                return (1,0,4)
            elif minor_version == "d":
                return (1,0,10)
            elif minor_version == "e":
                return (1,1,3)
            elif minor_version == "f":
                return (1,1,6)
        
        if major_version == "i":
            if minor_version == "a":
                return (1,2,0)
            elif minor_version == "b":
                return (1,2,1)

        raise ValueError("Unknown file format {version}".format(
            version=self.version))
            


# ============================================================================
#

class BackupFile(object):
    """A file that is going to be backed up
    """


    def __init__(self, file_path, host=None, md5=None, component=None):
        
        self.file_path = component.file_path if component is not \
            None else file_path
        self.component = component or SSTableComponent(file_path)
        self.host = host or socket.getfqdn()
        self.md5 = md5 or file_util.file_md5(file_path)
        
    def __str__(self):
        return "BackupFile {file_path}: host {host}, md5 {md5}, "\
            "{component}".format(**vars(self))
    
    def serialise(self):
        """Serialises the instance to a dict
        """
        
        return {
            "host" : self.host,
            "md5" : self.md5, 
            "cassandra_version" : TARGET_VERSION, 
            "component" : self.component.serialise()
        }
        
    @classmethod
    def backup_keyspace_dir(self, host, keyspace):
        """Gets the directory to that contains backups for the specified 
        ``host`` and ``keyspace``. 
        """

        return os.path.join(*(
            "hosts",
            host,
            keyspace
        ))
        
    @property
    def backup_path(self):
        """Gets the path to backup this file to. 
        """

        return os.path.join(*(
            "hosts",
            self.host,
            self.component.keyspace,
            self.component.cf,
            self.component.file_name,
        ))

# ============================================================================
#

class RestoreFile(object):
    """A file that has been backed up and it going to be restored.
    """


    def __init__(self, host=None, md5=None, component=None):
        self.component = component
        self.host = host 
        self.md5 = md5
    
    @classmethod
    def deserialise(cls, data):
        """Deserialise the data from a BackupFile to create a RestoreFile."""
        
        assert data
        return cls(
            host=data["host"], 
            md5=data["md5"], 
            component=SSTableComponent.deserialise(data["component"])
        )
        
    @property
    def restore_path(self):
        """Gets the path to restore this file to formatted for the current 
        ``TARGET_VERSION``.
        
        """
                
        if TARGET_VERSION < (1, 1, 0):
            # Pre 1.1 path was keyspace/sstable
            return os.path.join(*(
                self.keyspace,
                self.file_name,
            ))
        # after 1.1  path was keyspace/cf/sstable
        return os.path.join(*(
            self.keyspace,
            self.cf_name,
            self.file_name,
        ))
        

# ============================================================================
#

class KeyspaceBackup(object):
    """A backup set for a particular keyspace.
    """
    log = logging.getLogger("%s.%s" % (__name__, "KeyspaceBackup"))

    def __init__(self, data_dir, keyspace, host=None, timestamp=None, 
        backup_name=None, ks_files=None):
        
        self.keyspace = keyspace
        self.host = host or socket.getfqdn()
        self.timestamp = timestamp or dt_util.now()
        self.backup_name = backup_name or "{ts}-{keyspace}-{host}".format(
            ts=_to_safe_datetime_fmt(self.timestamp), 
            keyspace=keyspace, host=self.host)
        
        if ks_files is None and data_dir:
            self.ks_files = self._list_files(data_dir, keyspace) 
        else:
            self.ks_files = ks_files
            
    def serialise(self):
        """Return manifest that desribes the backup set."""
        
        files = {
            key : [component.serialise() for component in value]
            for key, value in self.ks_files.iteritems()
        }
        return {
            "host" : self.host,
            "keyspace" : self.keyspace,
            "timestamp" : dt_util.to_iso(self.timestamp),
            "name" : self.backup_name,
            "ks_files" : files
        }

    @classmethod
    def deserialise(cls, data):
        """Create an instance from the ``data`` dict. """
        
        assert data
        files = {
            key : [SSTableComponent.deserialise(comp) for comp in value]
            for key, value in data["ks_files"] 
        }
        return cls(None, data["keyspace"], host=data["host"], 
            timestamp=dt_util.from_iso(data["timestamp"]), 
            backup_name=data["name"], ks_files=files)
            
    def _list_files(self, data_dir, keyspace):
        """Gets a list of all sstable components in the ``keyspace`` under 
        the ``data_dir``.
        
        Returns a dict of {cf : [SSTableComponent,]}
        """
        
        if TARGET_VERSION < (1,1,0):
            # All files in the keyspace dir
            search_dirs = [os.path.join(data_dir, keyspace)]
        else:
            # Different dir for each CF.
            try:
                _, search_dirs, _ = os.walk(os.path.join(
                    data_dir, keyspace)).next()
            except (StopIteration):
                search_dirs = []

        ks_files = {}
        for search_dir in search_dirs:
            try:
                _, cf_files, _ = os.walk(search_dir).next() 
            except (StopIteration):
                cf_files = []
                
            for cf_file in cf_files:
                full_path = os.path.join(search_dir, cf_file)
                try:
                    component = SSTableComponent(full_path)
                except (ValueError):
                    # not a valid file name
                    pass
                else:
                    if not component.temporary:
                        ks_files.setdefault(component.cf, []).append(
                            component)
        return ks_files
   
    @classmethod
    def from_backup_name(cls, backup_name):
        """Create a KeyspaceBackup from a backup name. 
        
        The object does not contain a ks_files list."""
        
        # format is timestamp-keyspace-host
        # host may have "-" parts so only split the first two tokens 
        # from the name.
        tokens = backup_name.split("-", 2)

        safe_ts = tokens.pop(0)
        keyspace = tokens.pop(0)
        host = tokens.pop(0)
        assert not tokens

        # expecting 2012_10_22T14_26_57_871835 for the safe TS. 
        timestamp = _from_safe_datetime_fmt(safe_ts)
        return cls(None, keyspace, host=host, timestamp=timestamp)

    @classmethod
    def from_backup_path(cls, backup_path):
        _, local = os.path.split(backup_path)
        backup_name, _ = os.path.splitext(backup_path)
        return cls.from_backup_name(backup_name)

    @classmethod
    def backup_keyspace_dir(cls, keyspace):
        """Returns the backup dir used for the ``keyspace``.
        
        Manifests are not stored in this path, they are in 
        :attr:`backup_day_dir`
        """
        
        return os.path.join(*(
            "cluster",
            keyspace
        ))
        
    @classmethod
    def backup_day_dir(cls, keyspace, host, day):
        """Returns the backup dir used to store manifests for the 
        ``keyspace`` and ``host`` on the datetime ``day``"""
        
        
        return os.path.join(*(
            "cluster",
            keyspace,
            str(day.year),
            str(day.month),
            str(day.day),
            host
        ))
        
    @property
    def backup_path(self):
        """Gets the  path to backup the keyspace manifest to."""
        return os.path.join(
            self.backup_day_dir(self.keyspace, self.host, self.timestamp), 
            "%s.json" % (self.backup_name,)
        )

    def yield_file_names(self):
        """ 
        """

        assert self.column_families is not None

        for cf_name, cf_file_names in self.column_families.iteritems():
            for file_name in cf_file_names:
                yield file_name
