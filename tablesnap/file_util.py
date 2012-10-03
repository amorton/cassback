"""Utilities for working with Cassndra files."""

import os.path
import re


DATA_COMPONENT = "Data.db"
PRIMARY_INDEX_COMPONENT = "Index.db"
FILTER_COMPONENT = "Filter.db"
COMPRESSION_INFO_COMPONENT = "CompressionInfo.db"
STATS_COMPONENT = "Statistics.db"
DIGEST_COMPONENT = "Digest.sha1"
COMPACTED_MARKER = "Compacted"

TEMPORARY_MARKER = "tmp"

FILE_VERSION_PATTERN = re.compile("[a-z]+")

class Descriptor(object):

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
        """Parses ``file_path`` to create a :cls:`FileDescription`.

        `file_path` may be a path or a file name 
        """
        _, file_name = os.path.split(file_path)

        tokens = file_name.split("-")
        i = 0

        ks_name = tokens.pop(0)
        cf_name = tokens.pop(0)

        temporary = tokens[0] == TEMPORARY_MARKER:
        if temporary:
            tokens.pop(0)

        if FILE_VERSION_PATTERN.match(tokens[0]):
            version = tokens.pop()
        else:
            # legacy
            version = "a"

        generation = int(tokens.pop(0))
        component = tokens.pop(0)

        return (component, Descriptor(ks_name, cf_name, version, generation, 
            temporary))

def is_not_temp(file_path):
    return Descriptor.from_file_path(file_path).temporary == False




