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

import sqlite3
import datetime
import argparse
import os

class usagedb(object):
    def __init__(self, project, year=datetime.date.today().year):
        """
        Connect to a usage database
        """
        path = '/short/public/aph502/.data/usage_%s_%s.db'%(project, year)
        self.c = sqlite3.connect(path)

    def debug(self):
        """
        Enable printing out each sql call
        """
        self.c.set_trace_callback(print)

    def short(self, count=10, date=None):
        """
        Print the top /short users
        """
        if date is None:
            date = datetime.date.today() - datetime.timedelta(days=1)
        q = self.c.execute("""
            SELECT SUM(size), user.fullname
            FROM shortusage
            JOIN user ON shortusage.user = user.id
            WHERE shortusage.scandate = ?
            GROUP BY user.id ORDER BY sum(size) DESC
            LIMIT ?;
        """, (date, count))
        return q

    def gdata(self, count=10, date=None):
        """
        Print the top /g/data users
        """
        if date is None:
            date = datetime.date.today() - datetime.timedelta(days=1)
        q = self.c.execute("""
            SELECT SUM(size), user.fullname
            FROM gdatausage
            JOIN user ON gdatausage.user = user.id
            WHERE gdatausage.scandate = ?
            GROUP BY user.id ORDER BY sum(size) DESC
            LIMIT ?;
        """, (date, count))
        return q

    def ishort(self, count=10, date=None):
        """
        Print the top /short users
        """
        if date is None:
            date = datetime.date.today() - datetime.timedelta(days=1)
        q = self.c.execute("""
            SELECT SUM(inodes), user.fullname
            FROM shortusage
            JOIN user ON shortusage.user = user.id
            WHERE shortusage.scandate = ?
            GROUP BY user.id ORDER BY sum(inodes) DESC
            LIMIT ?;
        """, (date, count))
        return q

    def igdata(self, count=10, date=None):
        """
        Print the top /g/data users
        """
        if date is None:
            date = datetime.date.today() - datetime.timedelta(days=1)
        q = self.c.execute("""
            SELECT SUM(inodes), user.fullname
            FROM gdatausage
            JOIN user ON gdatausage.user = user.id
            WHERE gdatausage.scandate = ?
            GROUP BY user.id ORDER BY sum(inodes) DESC
            LIMIT ?;
        """, (date, count))
        return q

def print_short(db, args):
    for size, name in db.short(count=args.count):
        print("% 7d GiB\t %s"%(size/1024**3, name))

def print_ishort(db, args):
    for count, name in db.ishort(count=args.count):
        print("%.2e\t %s"%(count, name))

def print_gdata(db, args):
    for size, name in db.gdata(count=args.count):
        print("% 7d GiB\t %s"%(size/1024**3, name))

def print_igdata(db, args):
    for count, name in db.igdata(count=args.count):
        print("%.2e\t %s"%(count, name))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-P', default=os.environ['PROJECT'])
    parser.add_argument('--year', default=datetime.date.today().year, type=int)
    parser.add_argument('--count', default=10, type=int)

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

    db = usagedb(args.project, args.year)
    args.func(db, args)

if __name__ == '__main__':
    main()
