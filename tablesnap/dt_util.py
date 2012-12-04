"""Functions for working with date's n time n things like that. 
"""

import datetime


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
"""ISO string format"""

DT_INPUT_FORMATS= [
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S", 
    "%Y-%m-%d"
]
"""Formats to accept as datetime input. """

def now_iso():
    """Get the current date/time formatted as ISO."""
    return to_iso(datetime.datetime.now())
    
def to_iso(dt):
    """Convert the `dt` datetime to iso format. 
    """
    return dt.strftime(ISO_FORMAT)

def from_iso(dt_str):
    """Convert the iso ``dt_str`` to a datetime. 
    """
    return datetime.datetime.strptime(dt_str, ISO_FORMAT)

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