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
import re
import shutil
from UsageDataset import *
from DBcommon import extract_num_unit, parse_size, mkdir, archive, datetoyearquarter

databases = {}
dbfileprefix = '.'
verbose = False

def parse_short_file(filename):

    db = None
    
    with open(filename) as f:

        # Need this loop to support old method of having multiple dumps per file
        while True:
            # Need this try block to gracefully exit the above loop when end of file
            try:
                for line in f:
                    if line.startswith("%%%%%%%%%%%%%%%%%"):
                        # Grab date string
                        date = datetime.datetime.strptime(f.next().strip(os.linesep), "%a %b %d %H:%M:%S %Z %Y")
                        year, quarter = datetoyearquarter(date)
                        # Gobble another line
                        line = f.next()
                        break
                    else:
                        next

                # Assume a certain structure ....
                line = f.next()
                project = line.split()[4].strip(':')
                if not project in databases:
                    dbfile = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,date.year))
                    databases[project] = ProjectDataset(project,dbfile)
                db = databases[project]

                # Gobble the three header lines
                line = f.next(); line = f.next(); line = f.next()

                for line in f:
                    try:
                        (folder,user,size,inodes,scandate) = line.strip(os.linesep).split() 
                    except:
                        break
                    db.adduser(user)
                    if verbose: print('Adding short ',folder,user,size,inodes,scandate)
                    db.addshortusage(folder,user,parse_size(size.upper()),inodes,scandate)
            except:
                break

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Parse short file dumps")
    parser.add_argument("-d","--directory", help="Specify directory to find dump files", default=".")
    parser.add_argument("-v","--verbose", help="Verbose output", action='store_true')
    parser.add_argument("inputs", help="dumpfiles", nargs='*')
    args = parser.parse_args()

    verbose = args.verbose

    for f in args.inputs:
        if verbose: print(f)
        try:
            parse_short_file(f);
        except:
            raise
        else:
            archive(f)
