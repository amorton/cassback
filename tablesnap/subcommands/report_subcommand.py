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

import datetime
import itertools
import logging
import os.path

from tablesnap import dt_util
from tablesnap.subcommands import subcommands

# ============================================================================
# Survey Report - Report on the results of a survey. 

class SurveyReportSubCommand(subcommands.SubCommand):
    """
    """

    log = logging.getLogger("%s.%s" % (__name__, "SurveyReportSubCommand"))

    command_name = "survey-report"
    command_help = "Report on a survey endpoint output."
    command_description = "Report on a survey endpoint output."

    @classmethod
    def add_sub_parser(cls, sub_parsers):
        """
        """
        
        #TODO: convert from UTC
        
        sub_parser = super(SurveyReportSubCommand, cls).add_sub_parser(
            sub_parsers)

        sub_parser.add_argument("survey_path", 
            help="Path to the survey file to report on.")

        sub_parser.add_argument("--extrapolate-to",
            dest='extrapolate_to', default="now",
            help="Date time to extrapolate usage to.")

        sub_parser.add_argument("--show-extrapolations",
            dest="show_extrapolations", default=False, action="store_true",
            help="Show extrapolated usage in the report.")
            
        return sub_parser

    def __init__(self, args):
        self.args = args

    def __call__(self):

        
        hours = self._summarise_hours()
        
        if not hours:
            return (0, "No activity in survey log {args.survey_path}".format(
                args=self.args))
        
        self._extrapolate_hours(hours)

        str_builder = []
        write = str_builder.append
        
        write("Hours")
        write("")
        pattern = "{h.formatted_start:>15} {h.bytes:>15} "\
                "{h.max_bytes:>15} {h.trans_in:>15} "\
                "{h.trans_out:>15} {h.activity}"
        write(pattern.format(h=HourSummaryHeader))
        
        for hour in hours:
            if hour.activity or self.args.show_extrapolations:
                write(pattern.format(h=hour))

        write("")
        write("Usage summary")
        write("")
        
        month_key = lambda x : "{start.year}-{start.month}".format(
            start=x.start)
        for year_month, month_hours in itertools.groupby(hours, month_key):
            # It's an iterator
            month_hours = list(month_hours)
            
            # byte hours is max bytes for each hour
            byte_hours = sum(
                h.max_bytes
                for h in month_hours
            )
        
            # GB hours is GB's stored per hour. 
            one_gb = (1024**3) * 1.0
            hours_per_month = 744
            gb_months = byte_hours / one_gb / hours_per_month
        
            # total trans in and out 
            trans_in_gb = sum (
                h.trans_in
                for h in month_hours
            ) / one_gb

            trans_out_gb = sum (
                h.trans_out
                for h in month_hours
            ) / one_gb
            
            write("For Month {ym}.".format(ym=year_month))
            write("Byte-Hours   : {byte_hours:>25}".format(byte_hours=byte_hours))
            write("GB-Months    : {gb_months:>25.2}".format(gb_months=gb_months))
            write("GB Trans in  : {trans_in_gb:>25.2}".format(trans_in_gb=trans_in_gb))
            write("GB Trans out : {trans_out_gb:>25.2}".format(trans_out_gb=trans_out_gb))
                
        return (0, "\n".join(str_builder))

    def _summarise_hours(self):
        """Collect an hour summary for all lines in the log.
        """
        
        hours = []
        hour = None
        with open(self.args.survey_path, "r") as f:
            for line in f.readlines():
                tokens = line.split()
                
                dt = dt_util.from_iso(tokens.pop(0))
                op = tokens.pop(0)
                # ignore the src, "to", dest, and "size" tokens
                tokens.pop(0)
                tokens.pop(0)
                tokens.pop(0)
                tokens.pop(0)
                # Get the byte size
                bytes = int(tokens.pop(0))
                
                while hour is None or not hour.contains(dt):
                    if hour is None:
                        hour = HourSummary(HourSummary.find_hour(dt))
                    else:
                        assert dt > hour.start
                        hour = hour.next_hour()
                    hours.append(hour)
                    
                if op == "stored":
                    hour.incr_bytes(bytes)
                    hour.trans_in += bytes
                elif op == "restored":
                    hour.decr_bytes(bytes)
                    hour.trans_out += bytes
                elif op == "removed":
                    hour.decr_bytes(bytes)
                else:
                    raise RuntimeError("Unknown operation {op}".format(op=op))
        return hours

    def _extrapolate_hours(self, hours):
        """Add hours to ``hours`` to extrapolate the total usage.
        
        Updates ``hours`` in place. 
        """
        
        
        if self.args.extrapolate_to == "now":
            to_dt = datetime.datetime.now()
        else:
            to_dt = dt_util.parse_date_input(
                self.args.extrapolate_to)
                
        while hours[-1].start < to_dt and not hours[-1].contains(to_dt):
            hours.append(hours[-1].next_hour())
        return 

class HourSummaryHeader(object):

    formatted_start = "Hour"
    bytes = "Bytes"
    max_bytes = "Max Bytes"
    trans_out = "Trans. Out" 
    trans_in = "Trans. In"
    req_put = "Put Reqs."
    req_get = "Get Reqs."
    req_del = "Del Reqs."
    activity = "Activity"
    
class HourSummary(object):
    """Summary of the data stored and transfered in an hour."""
    
    @classmethod
    def find_hour(cls, dt):
        """Return a ``datetime`` for the hour slice ``dt`` is from.
        """
        return datetime.datetime(dt.year, dt.month, dt.day, dt.hour)
    
    def __init__(self, start):
        
        # start hour as a datetime.
        self.start = start
        self.bytes = 0
        self.max_bytes = 0
        self.trans_out = 0 
        self.trans_in = 0
        self.req_put = 0
        self.req_get = 0
        self.req_del = 0
        
        # Flag if this hour is an extrapolation after the last recored 
        # activity. 
        self.activity = False
        
    @property
    def formatted_start(self):
        return self.start.strftime("%Y-%m-%dT%H:%M")
        
    def incr_bytes(self, bytes):
        next_bytes = self.bytes + bytes
        self.max_bytes = max(self.bytes, next_bytes)
        self.bytes = next_bytes
        self.activity = True
        return
        
    def decr_bytes(self, bytes):
        self.bytes -= bytes
        self.activity = True
        return
        
    def contains(self, dt):
        """Returns ``True`` if the datetime ``dt`` is contained in 
        this hour."""
        
        return self.find_hour(dt) == self.start

    def next_hour(self):
        """Returns the next hour summary after this. 
        
        The next hour starts with the same :attr:`bytes` and 
        :attr:`max_bytes` but the other counters are zeroed."""
        
        next_hour = HourSummary(self.start + datetime.timedelta(0, 3600))
        next_hour.bytes = self.bytes
        #reset the max bytes to the current size
        next_hour.max_bytes = self.bytes
        return next_hour
        




