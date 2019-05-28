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

bytes_to_gbytes = 1024**3

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-P', default=os.environ['PROJECT'])
    parser.add_argument('--period', '-p', type=str)
    parser.add_argument('--count', default=10, type=int)
    parser.add_argument('--percent', default=False, action='store_true')
    parser.add_argument('--short', action='store_true')
    parser.add_argument('--gdata', action='store_true')
    parser.add_argument('--measure', choices=['size','inodes'], default='size')

    args = parser.parse_args()

    if args.period is not None:
        year, quarter = args.period.split(".")
    else:
        year, quarter = datetoyearquarter(datetime.datetime.now())

    path = 'sqlite:////short/public/aph502/.data/usage_%s_%s.db'%(args.project, year)
    db = ProjectDataset(args.project, path)

    storagepoints = []
    if args.gdata:
        storagepoints.append('gdata')
    if args.short:
        storagepoints.append('short')
    if not (args.gdata or args.short):
        storagepoints = ['short', 'gdata']

    for storagepoint in storagepoints:

        if args.measure == 'inodes':
            name = "{} inodes ".format(storagepoint)
            scale = 1
            format_ = '%i'
        else:
            if args.percent:
                name = "{}".format(storagepoint)
            else:
                name = "{} (GB)".format(storagepoint)
            scale = 1024 ** 3 # 1 GB
            format_ = '%.0f'

        if args.percent:
            system = 'raijin'
            if storagepoint == 'gdata':
                system = 'global'

            format_ = "{0:.0f} %".format
            grant, igrant = db.getsystemstorage(system, storagepoint, year, quarter)

            if args.measure == 'size':
                scale = grant / 100.
            elif args.measure == 'inodes':
                scale = igrant / 100.

        df = db.getstorage(year, quarter, storagept=storagepoint, datafield=args.measure)
        usertotal = df.ix[-1]
        total = sum(usertotal)

        report = usertotal.sort_values(ascending=False).head(args.count)
        report.at['TOTAL'] = total

        print(report.divide(scale).to_frame(name).to_string(float_format=format_))

if __name__ == '__main__':
    main()
