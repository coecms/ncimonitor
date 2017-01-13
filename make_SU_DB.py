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

import argparse
import pwd
import datetime
import sys
import os
from math import log
import re
import gzip
import shutil
from UsageDataset import *
from DBcommon import extract_num_unit, parse_size, mkdir, archive, parse_inodenum

databases = {}
dbfileprefix = '.'
verbose = False

def parse_SU_file(filename):

    insystem = False; instorage = False; inuser = False
    
    with open(filename) as f:

        year = ''; quarter = ''
        for line in f:
            if line.startswith("%%%%%%%%%%%%%%%%%"):
                # Grab date string
                date = datetime.datetime.strptime(f.next().strip(os.linesep), "%a %b %d %H:%M:%S %Z %Y").date()
            elif line.startswith("Usage Report:") and "Compute" in line:
                words = line.split()
                project = words[2].split('=')[1]
                year, quarter = words[4].split('=')[1].split('.')
                print year, quarter
                startdate, enddate = words[5].split('-')
                startdate = datetime.datetime.strptime(startdate.strip('('),"%d/%m/%Y").date()
                enddate = datetime.datetime.strptime(enddate.strip(')'),"%d/%m/%Y").date()
                if not project in databases:
                    dbfile = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,year))
                    databases[project] = ProjectDataset(project,dbfile)
                db = databases[project]
                db.addquarter(year,quarter,startdate,enddate)
            elif line.startswith("Total Grant:"):
                total = line.split(":")[1]
                # Grant is stored in KSU, parse_size translates to SU, so divide by zero
                db.addgrant(year,quarter,parse_size(total.upper(),u='SU')/1000.)
            elif line.startswith("System       Queue"):
                insystem = True
                f.next()
            elif insystem:
                try:
                    (system,queue,weight,usecpu,usewall,usesu,tmp,tmp,tmp) = line.strip(os.linesep).split() 
                except:
                    insystem = False
                    continue
                db.addsystemqueue(system,queue,weight)
                db.addprojectusage(date,system,queue,usecpu,usewall,usesu)
            elif line.startswith("Batch Queue Usage per User"):
                inuser = True
                # Gobble three lines
                f.next(); f.next(); f.next()
            elif inuser:
                try:
                    (user,usecpu,usewall,usesu,tmp) = line.strip(os.linesep).split() 
                except:
                    inuser = False
                    continue
                db.adduser(user)
                if verbose: print 'Add usage ',date,user,usecpu,usewall,usesu
                db.adduserusage(date,user,usecpu,usewall,usesu)
            elif line.startswith("System    StoragePt"):
                instorage = True
                f.next()
            elif instorage:
                try:
                    (systemname,storagept,grant,tmp,tmp,igrant,tmp,tmp) = line.strip(os.linesep).split() 
                except:
                    instorage = False
                    continue
                print(year, quarter, systemname, storagept, grant.upper(), parse_size(grant.upper()))
                db.addsystemstorage(systemname,storagept,year,quarter,parse_size(grant.upper()),parse_inodenum(igrant))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Parse usage dump files")
    parser.add_argument("-d","--directory", help="Specify directory to find dump files", default=".")
    parser.add_argument("-v","--verbose", help="Verbose output", action='store_true')
    parser.add_argument("inputs", help="dumpfiles", nargs='*')
    args = parser.parse_args()

    verbose = args.verbose

    for f in args.inputs:
        if verbose: print f
        try:
            parse_SU_file(f);
        except:
            raise
        else:
            archive(f)
