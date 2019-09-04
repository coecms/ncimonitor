#!/usr/bin/env python

"""
Copyright 2019 ARC Centre of Excellence for Climate Systems Science

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
import sys
import os
from math import log
import re
import gzip
import shutil
from .UsageDataset import *
from .DBcommon import extract_num_unit, parse_size, mkdir, archive, parse_inodenum

databases = {}
dbfileprefix = '.'

def parse_account_dump_file(filename, verbose, db=None, dburl=None):

    with open(filename) as f:

        insystem = False; instorage = False; inuser = False; inusage=False
        project = None
    
        year = ''; quarter = ''
        system = ''; date = ''
        storagetype = ''; storagepoint = ''

        for line in f:
            line = line.rstrip(os.linesep)
            if verbose: print(line)
            if line.startswith("%%%%%%%%%%%%%%%%%"):
                # Grab date string
                date = datetime.datetime.strptime(f.readline().rstrip(os.linesep), "%a %b %d %H:%M:%S %Z %Y").date()
            elif line.startswith("Usage Report:") and "Compute" in line:
                words = line.split()
                project = words[2].split('=')[1]
                year, quarter = words[4].split('=')[1].split('.')
                print(project, year, quarter)
                startdate, enddate = words[5].split('-')
                startdate = datetime.datetime.strptime(startdate.strip('('),"%d/%m/%Y").date()
                enddate = datetime.datetime.strptime(enddate.strip(')'),"%d/%m/%Y").date()
                db.addquarter(year,quarter,startdate,enddate)
            elif line.startswith("System        Queue"):
                insystem = True
                f.readline()
            elif insystem:
                try:
                    (system,queue,weight,usecpu,usewall,usesu,tmp,tmp,tmp) = line.rstrip(os.linesep).split() 
                except:
                    insystem = False
                    continue
                db.addsystemqueue(system, queue, weight)
                if verbose: print('Add project usage ',date,system,queue,usecpu,usewall,usesu)
                db.addprojectusage(project, system, queue, date, usecpu, usewall, usesu)
            elif line.startswith("Batch Queue Usage per User"):
                inuser = True
                # Gobble three lines
                f.readline(); f.readline(); f.readline()
            elif inuser:
                try:
                    (user,usecpu,usewall,usesu,efficiency) = line.split() 
                except:
                    inuser = False
                    continue
                if verbose: print('Add usage ',date,user,usecpu,usewall,usesu,efficiency)
                db.adduserusage(project, user, date, usecpu, usewall, usesu, efficiency.strip('%'))
            elif line.startswith("CPU resource:"):
                inusage = True
                system = line.split('=')[1]
                if verbose: print('inusage: ',inusage)
                # Gobble header line
                f.readline()
                f.readline()
                f.readline()
                f.readline()
            elif inusage:
                # Stakeholder    Grant           Used      Available       Price         CPU         CPU         CPU
                #                (KSU)          (KSU)          (KSU)  (per SU) $    Credit $      Used $   Balance $
                # MAS-FlagshipCLEX-grant         160.00          28.68         131.32       0.040           -     1147.08           -
                try:
                    tmp = line.split() 
                    assert(len(tmp)==8)
                    (scheme,granttype) = tmp[0].rsplit('-',1)
                    grantsu  = tmp[1]
                    usesu    = tmp[2]
                    costpersu = tmp[3]
                except:
                    inusage = False
                    continue

                # Don't bother tracking bonus allocations
                if granttype == 'grant':

                    if verbose: print('Add scheme ', project, scheme)
                    db.addscheme(scheme)

                    if verbose: print('Add scheme grants ', project, system, scheme, year, quarter, date, grantsu)
                    db.addusagegrant(project, system, scheme, year, quarter, date, grantsu)

            elif line.startswith("Storage resource:"): # Storage resource: System=dmf
                instorage = True
                system = line.split('=')[1]
                resources_read = 0
            elif instorage:
                if line.lstrip().startswith("Resource="): # Resource=massdata-capacity
                    (storagepoint, storagetype) = line.split()[0].split('=')[1].rsplit('-',1)
                    storagetype=storagetype.rstrip(',')
                    # Gobble header
                    f.readline(); f.readline(); 
                    grant_units = f.readline().split()[1].strip('()')
                    resources_read += 1
                else:
                    try:
                        (scheme_granttype,grant,used,available,max_used,ave_used,
                        price,credit,storage_cost,storage_balance) = line.split() 
                        (scheme, granttype) = scheme_granttype.rsplit('-',1)
                        if storagetype == 'inodes':
                            parsed_value = parse_size(grant+grant_units,b=1000,u='')
                        if storagetype == 'capacity':
                            parsed_value = round(parse_size(grant+grant_units),0)
                        if verbose: print('Add project storage grant',project, system, storagepoint, scheme, 
                                           year, quarter, date, storagetype, parsed_value)
                        db.addstoragegrant(project, system, storagepoint, scheme, year, quarter, 
                                           date, storagetype, parsed_value)
                    except:
                        resources_read =+ 1
                        if resources_read == 2:
                            instorage = False
                        # Add the collected data
                        continue


# Storage resource: System=dmf
#      Resource=massdata-capacity, Current Usage formula=P1(MAS-FlagshipC-grant:62+ClimateLIEF-grant:938)
#      --------------------------------------------------------------------------------------------------------------------------------------------
#      Stakeholder                  Grant        Used   Available    Max Used    Average Used          Price     Storage     Storage        Storage
#                                    (TB)        (TB)        (TB)        (TB)            (TB)   (GB/month) $    Credit $      Used $      Balance $
#      ClimateLIEF-grant            60.00       26.50       33.50       26.50           28.85          0.019           -       18.11              -
#      MAS-FlagshipC-grant           3.95        1.75        2.20        1.75            1.91          0.019           -        1.20              -
#      --------------------------------------------------------------------------------------------------------------------------------------------
#      Total                        63.95       28.26       35.70       28.26           30.76                          -       19.30              -
#
#      Resource=massdata-inodes, Current Usage formula=P1(MAS-FlagshipC-grant:68+ClimateLIEF-grant:932)
#      --------------------------------------------------------------------------------------------------------------------------------------------
#      Stakeholder                  Grant        Used   Available    Max Used    Average Used          Price     Storage     Storage        Storage
#                                     (K)         (K)         (K)         (K)             (K)  (per month) $    Credit $      Used $      Balance $
#      ClimateLIEF-grant          4406.00        7.34     4398.66        7.34            7.99          0.000           -        0.00              -
#      MAS-FlagshipC-grant         323.00        0.54      322.46        0.54            0.58          0.000           -        0.00              -
#      --------------------------------------------------------------------------------------------------------------------------------------------
#      Total                      4729.00        7.88     4721.12        7.88            8.57                          -        0.00              -



def main(args):

    verbose = args.verbose

    db = None
    if args.dburl:
        db = ProjectDataset(dburl=args.dburl)

    for f in args.inputs:
        if verbose: print(f)
        try:
            parse_account_dump_file(f, verbose, db=db)
        except:
            raise
        else:
            if not args.noarchive:
                archive(f)

def parse_args(args):
    """
    Parse arguments given as list (args)
    """
    parser = argparse.ArgumentParser(description="Parse usage dump files")
    parser.add_argument("-d","--directory", help="Specify directory to find dump files", default=".")
    parser.add_argument("-v","--verbose", help="Verbose output", action='store_true')
    parser.add_argument("-db","--dburl", help="Database file url", default=None)
    parser.add_argument("-n","--noarchive", help="Database file url", action='store_true')
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

