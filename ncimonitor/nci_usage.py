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


def print_short(db, args):
    sep = args.separator
    for row in db.top_usage('short', measure='size', count=args.count):
        print("% 7d GiB%s%s%s%s"%(row['measure']/1024**3, sep, row['username'], sep, row['fullname']))

def print_ishort(db, args):
    sep = args.separator
    for row in db.top_usage('short', measure='inodes', count=args.count):
        print("%.2e%s%s%s%s"%(row['measure'], sep, row['username'], sep, row['fullname']))

def print_gdata(db, args):
    sep = args.separator
    for row in db.top_usage('gdata', measure='size', count=args.count):
        print("% 7d GiB%s%s%s%s"%(row['measure']/1024**3, sep, row['username'], sep, row['fullname']))

def print_igdata(db, args):
    sep = args.separator
    for row in db.top_usage('gdata', measure='inodes', count=args.count):
        print("%.2e%s%s%s%s"%(row['measure'], sep, row['username'], sep, row['fullname']))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-P', default=os.environ['PROJECT'])
    parser.add_argument('--year', default=datetime.date.today().year, type=int)
    parser.add_argument('--count', default=10, type=int)
    parser.add_argument('--separator','-s', default='\t', type=str)

    subparsers = parser.add_subparsers(help='Commands')
    short = subparsers.add_parser('short', help='/short usage')
    short.set_defaults(func=print_short)

    gdata = subparsers.add_parser('gdata', help='/g/data usage')
    gdata.set_defaults(func=print_gdata)

    ishort = subparsers.add_parser('ishort', help='/short inodes')
    ishort.set_defaults(func=print_ishort)

    igdata = subparsers.add_parser('igdata', help='/g/data inodes')
    igdata.set_defaults(func=print_igdata)

    args = parser.parse_args()

    path = 'sqlite:////short/public/aph502/.data/usage_%s_%s.db'%(args.project, args.year)
    db = ProjectDataset(args.project, path)

    args.func(db, args)

if __name__ == '__main__':
    main()
