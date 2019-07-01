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
import pwd
import datetime
import os
import sys
import shutil
from .UsageDataset import *
from .DBcommon import extract_num_unit, parse_size, mkdir, archive, datetoyearquarter

databases = {}
dbfileprefix = '.'

def parse_gdata_file(filename, verbose, dburl=None):

    db = None

    storagept = ''
    storageptstring = 'gdata'

    start = filename.find('gdata')
    if start < 0:
        print('Could not find storage point (e.g gdata1) in filename {}'.format(filename))
        raise
    else:
        storagept = filename[start:start+len(storageptstring)+1]
        print('Storage Point: {}'.format(storagept))
    
    with open(filename) as f:

        parsing_usage = False

        for line in f:
            if verbose: print("> ",line)
            if line.startswith("%%%%%%%%%%%%%%%%%"):
                # Grab date string
                date = datetime.datetime.strptime(f.readline().strip(os.linesep), "%a %b %d %H:%M:%S %Z %Y")
                year, quarter = datetoyearquarter(date)
                continue

            if line.startswith("Usage details for project"):
                # Assume a certain structure ....
                project = line.split()[4].strip(':')
                if not project in databases:
                    if dburl is None:
                        dburl = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,year))
                    databases[project] = ProjectDataset(project,dburl)
                db = databases[project]

                # Gobble the three header lines
                for i in range(3):
                    line = f.readline()

                parsing_usage = True

                continue

            if parsing_usage:
                try:
                    (folder,user,size,inodes,scandate) = line.strip(os.linesep).split() 
                except:
                    print('Finished parsing usage data')
                    parsing_usage=False
                    continue
                db.adduser(user)
                if (verbose): print('Adding gdata ',folder,user,size,inodes,scandate)
                db.addgdatausage(project,storagept,folder,user,parse_size(size.upper()),inodes,scandate)


def main(args):

    verbose = args.verbose

    for f in args.inputs:
        if verbose: print(f)
        try:
            parse_gdata_file(f, verbose, args.dburl);
        except:
            raise
        else:
            pass
            # archive(f)

def parse_args(args):
    """
    Parse arguments given as list (args)
    """
    parser = argparse.ArgumentParser(description="Parse gdata file dumps")
    parser.add_argument("-d","--directory", help="Specify directory to find dump files", default=".")
    parser.add_argument("-v","--verbose", help="Verbose output", action='store_true')
    parser.add_argument("-db","--dburl", help="Database file url", default=None)
    parser.add_argument("inputs", help="dumpfiles", nargs='+')

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

