#!/usr/bin/env python

"""
Copyright 2015 ARC Centre of Excellence for Climate Systems Science

author: Aidan Heerdegen <aidan.heerdegen@anu.edu.au>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from __future__ import print_function

import argparse
import datetime
import json
import os
import pwd
import re
import shutil
from string import Template
import sys

# Local imports
from JobsDataset import *
from DBcommon import extract_num_unit, parse_size, mkdir, archive, datetoyearquarter

databases = {}
dbfileprefix = '.'
verbose = False

class DeltaTemplate(Template):
    delimiter = "%"

def strfdelta(tdelta, fmt):
    d = {"D": tdelta.days}
    hours, rem = divmod(tdelta.seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    d["H"] = '{:02d}'.format(hours)
    d["M"] = '{:02d}'.format(minutes)
    d["S"] = '{:02d}'.format(seconds)
    t = DeltaTemplate(fmt)
    return t.substitute(**d)

def pbs_str_to_date(datestring):
    """
    Parse a PBS datestamp that looks like this:

    'Tue Mar 12 09:45:37 2019'

    """
    return datetime.datetime.strptime(datestring, "%a %b %d %H:%M:%S %Y")
    # return datestring

def walltime_to_seconds(walltimestring):
    """
    Parse a PBS walltime like this into a time delta and return 
    in units of total seconds

    '09:45:37'

    """
    if walltimestring is None:
        return -1.

    (h, m, s) = walltimestring.split(':')
    return datetime.timedelta(hours=int(h), minutes=int(m), seconds=int(s)).total_seconds()

def maybe_get_time(info, timefield, must=False):
    time = None
    try:
        time = pbs_str_to_date(info[timefield])
    except KeyError:
        if must: raise
    return time

def strip_ml(text):
    """
    Strip markup language tags like this:
    <jsdl-hpcpa:Argument>/apps/payu/0.11.2/bin/payu-run</jsdl-hpcpa:Argument>
    from https://stackoverflow.com/a/4869782/4727812
    """
    if text is None:
        return None
    return re.sub('<[^<]+?>', '', text)

def parse_qstat_json_dump(filename, dbfile, verbose=False):

    db = JobsDataset("sqlite:///{}".format(dbfile))

    numrecords = db.getnumrecords()

    nentries = 0

    with open(filename) as f:

        data = json.load(f)

        if 'Jobs' in data:
            data = data['Jobs']

        for jobid, info in data.items():

            if jobid == '_default': continue

            try:
                # Strip off '.r-man2' suffix if it exists
                jobid = jobid.split('.')[0]

                # Must have
                ctime = maybe_get_time(info, 'ctime', must=True)
                qtime = maybe_get_time(info, 'qtime', must=True)
                mtime = maybe_get_time(info, 'mtime', must=True)

                # Store all times as offset from creation time in seconds
                qtime = (qtime - ctime).total_seconds()
                mtime = (mtime - ctime).total_seconds()

                """
                    B  Array job: at least one subjob has started.
                    E  Job is exiting after having run.
                    F  Job is finished.
                    H  Job is held.
                    M  Job was moved to another server.
                    Q  Job is queued.
                    R  Job is running.
                    S  Job is suspended.
                    T  Job is being moved to new location.
                    U  Cycle-harvesting job is suspended due to keyboard activity.
                    W  Job is waiting for its submitter-assigned start time to be reached.
                    X  Subjob has completed execution or has been deleted.
                """

                # Put in some logic checking for job_state?
                stime = maybe_get_time(info, 'stime')

                # Needed to calculate time in the queue
                if stime is None:
                    start = datetime.datetime.now()
                    stime = -1.
                else:
                    start = stime
                    stime = (stime - ctime).total_seconds()

                # Create a derived field which is the total time spend queuing before
                # job started
                waitime = (start - ctime).total_seconds()

                # year = int(info['qtime'].split()[-1])
                year = ctime.year

                username = info['Job_Owner'].split('@')[0]

                resources = info['Resource_List']
                resources_used = info.get('resources_used',{})

                maxwalltime = walltime_to_seconds(resources['walltime'])
                walltime = walltime_to_seconds(resources_used.get('walltime', None))
                maxmem = int(parse_size(resources.get('mem', '0b').upper()))
                ncpus = resources.get('ncpus', None)
                mem = int(parse_size(resources_used.get('mem', '0b').upper()))
                cputime = walltime_to_seconds(resources_used.get('cput', None))
                try:
                    cpuutil = cputime/(walltime*ncpus)
                except ZeroDivisionError:
                    cpuutil = -1.

                exe = strip_ml(info.get('executable', ''))
                arglist = strip_ml(info.get('argument_list', ''))
                subarglist = info.get('Submit_arguments', '')

                # Use -999 to signify no exit status
                exit_status =  info.get('Exit_status',-999)

                if verbose:
                    print(year, info['queue'], jobid, info['project'], username,
                        info['job_state'], info['Job_Name'], resources['jobprio'], exe, arglist + subarglist,
                        ctime, mtime, qtime, stime, waitime,
                        maxwalltime, maxmem, ncpus,
                        walltime, mem, cputime, cpuutil, exit_status)
                db.addjob(year, info['queue'], jobid, info['project'], username,
                        info['job_state'], info['Job_Name'], resources['jobprio'], exe, arglist + subarglist,
                        ctime, mtime, qtime, stime, waitime,
                        maxwalltime, maxmem, ncpus,
                        walltime, mem, cputime, cpuutil, exit_status)
                nentries += 1
            except:
                print("Error parsing {}".format(jobid))
                print(info)
                raise
                    
    newrecords = db.getnumrecords() - numrecords

    print("Found {} entries. Added {} new records, {} records updated or unchanged".format(nentries, newrecords, nentries - newrecords)) 

def main(args):

    verbose = args.verbose

    for f in args.inputs:
        print("Reading dumpfile: {}".format(f))
        try:
            parse_qstat_json_dump(f, args.database, verbose)
        except:
            raise
        else:
            # archive(f)
            pass

def parse_args(args):
    """
    Parse arguments given as list (args)
    """
    parser = argparse.ArgumentParser(description='Read PBS job information from qstat json file dumps and store in a database')
    parser.add_argument('-d','--directory', help='Specify directory to find dump files', default='.')
    parser.add_argument('-v','--verbose', help='Verbose output', action='store_true')
    parser.add_argument('-db','--database', help='Verbose output', default='jobs.db')
    parser.add_argument('inputs', help='dumpfiles', nargs='+')

    return parser.parse_args()

def main_parse_args(args):
    """
    Call main with list of arguments. Callable from tests
    """
    # Must return so that check command return value is passed back to calling routine
    # otherwise py.test will fail
    return main(parse_args(args))

def main_argv():
    """
    Call main and pass command line arguments. This is required for setup.py entry_points
    """
    main_parse_args(sys.argv[1:])

if __name__ == "__main__":

    main_argv()

