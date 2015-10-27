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

import sys
import os
import shutil
import re
import gzip

def extract_num_unit(s):
    # Match a number (possibly floating point 100.00 style) and a unit
    try:
        size, unit = re.findall('(\d+.\d+|\d+)\s?(\D*)$',s)[0]
    except:
        print 'Failed to match size string: ',s
        sys.exit()
    return float(size), unit

def pretty_size(n,pow=0,b=1024,u='B',pre=['']+[p for p in'KMGTPEZY']):
    pow,n=min(int(log(max(n*b**pow,1),b)),len(pre)-1),n*b**pow
    return "%%.%if %%s%%s"%abs(pow%(-pow-1))%(n/b**float(pow),pre[pow],u)
        
def parse_size(size,b=1024,u='B',pre=['']+[p for p in'KMGTPEZY']):
    """Parse human readable file sizes, e.g. 16.4TB. Only works for 2 char suffix"""
    pow = { k+u:v for v, k in enumerate(pre) }
    intsize, unit = extract_num_unit(size)
    return float(intsize)*(b**pow[unit])

def parse_inodenum(num):
    return parse_size(num,b=1000,u='')  

def mkdir(path):
    """Make a directory, without a race condition
    from http://stackoverflow.com/a/14364249
    """
    try: 
        os.mkdir(path)
    except OSError:
        if not os.path.isdir(path):
            raise

def archive(filepath,archive_dir='archive'):
    """Move dumpfile into archive directory, and compress it"""

    # Make sure we have a directory to archive to
    try:
        mkdir(archive_dir)
    except:
        print "Error making archive directory"
        return

    try:
        (dir, filename) = os.path.split(filepath)
        outfile = os.path.join(dir,archive_dir,filename)+'.gz'
        with open(filename, 'rb') as f_in, gzip.open(outfile, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    except Exception, e:
        print "Error archiving ",filepath
        print e
    else:
        try:
            os.remove(filepath)
        except:
            print "Error removing ",filepath

def datetoyearquarter(date):
    year = date.year
    # Convert month into year and quarter
    quarter = 'q{}'.format((int(date.month) - 1) / 3 + 1)
    return year, quarter
