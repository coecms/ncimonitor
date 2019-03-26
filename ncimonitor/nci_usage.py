#!/usr/bin/env python
"""
Copyright 2017 ARC Centre of Excellence for Climate Systems Science

author: Scott Wales <scott.wales@unimelb.edu.au>

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

import datetime
import argparse
import os
from .UsageDataset import ProjectDataset
from .DBcommon import datetoyearquarter


def print_usage(db, year, quarter, args):
    print(db.top_usage(year, 
                       quarter, 
                       args.storagepoint, 
                       args.measure, 
                       args.count).to_string(float_format=args.format))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-P', default=os.environ['PROJECT'])
    parser.add_argument('--period', '-p', type=str)
    parser.add_argument('--count', default=10, type=int)
    parser.add_argument('--separator','-s', default='\t', type=str)

    subparsers = parser.add_subparsers(help='Commands')
    short = subparsers.add_parser('short', help='/short usage')
    short.set_defaults(measure='size', storagepoint='short', format="%.0f GB")

    gdata = subparsers.add_parser('gdata', help='/g/data usage')
    gdata.set_defaults(measure='size', storagepoint='gdata', format="%.0f GB")

    ishort = subparsers.add_parser('ishort', help='/short inodes')
    ishort.set_defaults(measure='inodes', storagepoint='short', format="%i")

    igdata = subparsers.add_parser('igdata', help='/g/data inodes')
    igdata.set_defaults(measure='inodes', storagepoint='gdata', format="%i")

    args = parser.parse_args()

    if args.period is not None:
        year, quarter = args.period.split(".")
    else:
        year, quarter = datetoyearquarter(datetime.datetime.now())

    path = 'sqlite:////short/public/aph502/.data/usage_%s_%s.db'%(args.project, year)
    db = ProjectDataset(args.project, path)

    print_usage(db, year, quarter, args)

if __name__ == '__main__':
    main()
