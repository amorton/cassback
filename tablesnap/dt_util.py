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
"""Functions for working with date's n time n things like that. 
"""

import datetime
import time

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
"""ISO string format"""

DT_INPUT_FORMATS= [
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S", 
    "%Y-%m-%dT%H:%M", 
    "%Y-%m-%d"
]
"""Formats to accept as datetime input. """

def now():
    return datetime.datetime.utcnow()
    
def now_local():
    return datetime.datetime.now()
    
def now_iso():
    """Get the current date/time formatted as ISO."""
    return to_iso(now())
    
def to_iso(dt):
    """Convert the `dt` datetime to iso format. 
    """
    return dt.strftime(ISO_FORMAT)

def from_iso(dt_str):
    """Convert the iso ``dt_str`` to a datetime. 
    """
    return datetime.datetime.strptime(dt_str, ISO_FORMAT)

def to_utc(dt):
    """Convert a local datetime to utc."""
    time_tuple = time.gmtime(time.mktime(dt.timetuple()))
    return datetime.datetime(*time_tuple[0:6])

def parse_date_input(input, formats=DT_INPUT_FORMATS):
    """Parse the datetime input ``input`` using the ``formats``.
    
    If input cannot be parsed a :exc:`ValueError` is raised. 
    """

    for fmt in formats:
        try:
            return datetime.datetime.strptime(input, fmt)
        except (ValueError):
            pass
    raise ValueError("'%(input)s' is not a valid datetime." % vars())