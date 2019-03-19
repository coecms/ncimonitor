#!/usr/bin/env python

"""
Copyright 2019 ARC Centre of Excellence for Climate Extremes

author: Claire Carouge <c.carouge@unsw.edu.au>

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

# Notes for later:
# Remove default to 'all' for project. It's better to have a script or 
# bash function that calls if with all projects listed. Then default
# to [os.environ['PROJECT']]


from __future__ import print_function
import argparse
import os
from ncimonitor.nci_usage import *
from ncimonitor.UsageDataset import *
from ncimonitor.DBcommon import datetoyearquarter, parse_size, parse_inodenum
import sqlite3
import subprocess

BtoGiB=1024**3

class ProjectInfo(ProjectDataset):        

    def get_percent(self, totused, grant):
        ''' Calculate the percentage used '''
        PercUsed = totused/grant * 100 # percentage of storage used on gdata
        return(PercUsed)
        

    def get_mdss_usage(self, year):
        '''Temporary function to get mdss Used number from nci_account output'''

        # Call `nci_account` and save standard output
        a = subprocess.run(['nci_account','-P '+self.project],stdout=subprocess.PIPE,universal_newlines=True)

        # Split stdout in lines
        account_lines=a.stdout.split(os.linesep)

        # Find the storage data: first line for storage starts with System and contains StoragePt
        found_data=0
        usage=-999
        iusage=-999
        for line in account_lines:
            if line.startswith('dmf'):
                # That's the line!
                found_data = 1

                # To scrape the storage line:
                try:
                    (systemname,storagept,tmp,usage,tmp,tmp,iusage,tmp) = line.strip(os.linesep).split() 
                except:
                    print('impossible to format the massdata line correctly')
                    continue
                else:
                    break

        if found_data == 0:
            print("hmmm. no massdata storage found")

        # Parse human readable size string to float
        usage = parse_size(usage.upper())
        return (usage,parse_inodenum(iusage))

    def get_MdssInfo(self, year, quarter):
        '''To get the total usage of massdata in percentage for the project '''

        # Massdata usage data: for the moment, scrape nci_account output until the data is added to db.
        usage, iusage = self.get_mdss_usage(year)

        # Get grant:
        grant, igrant = self.getsystemstorage('dmf','massdata',year, quarter)

        # Get percentage
        MPercUsed = self.get_percent(usage, grant)

        MdssInfo={'grant':grant, 'used':usage, 'perc':MPercUsed}

        return(MdssInfo)

    def get_GdataInfo(self, year, quarter, dbfile, date=None):
        ''' To get the total usage of gdata in percentage for the project '''
        
        if date is None:
            date = datetime.date.today() - datetime.timedelta(days=1)

        dbfile = dbfile.replace('sqlite:///','')
        db_usage = sqlite3.connect(dbfile)
        totused = db_usage.execute("""
                SELECT SUM(size)
                FROM gdatausage
                WHERE gdatausage.scandate = ?
            """, (date,))
        
        # Get the total allocation and calculate the percentage
        # used. For total allocation, use getsystemstorage() from UsageDataset
        grant, igrant = self.getsystemstorage('global', 'gdata', year, quarter)
            
        for usage in totused:
            GPercUsed = self.get_percent(usage[0], grant)

        # Keep exact values and rounded ones for output
        GdataInfo={'grant':grant, 'used':usage[0], 'perc':GPercUsed}
        return(GdataInfo)
        
class UserInfo(usagedb):

    def print_gdata_perc(db, sep, count, grant,date=None):
        for size, username, name in db.gdata(count=count,date=date):
            print("% 2d%s% 7d GiB%s%s%s%s"%(size/grant*100, sep,size/BtoGiB, sep, username, sep, name))

def output_info(GdataInfo, MdssInfo, db1, date, count, project):

    print("Project: {0}".format(project))
    print("Gdata: {used:7.0f} GiB/{grant:7.0f} GiB, {perc_round}".format(used=GdataInfo['used']/BtoGiB, grant=GdataInfo['grant']/BtoGiB, perc_round=round(GdataInfo['perc'])))
    print("Mdss: {used:7.0f} GiB/{grant:7.0f} GiB, {perc_round}".format(used=MdssInfo['used']/BtoGiB, grant=MdssInfo['grant']/BtoGiB, perc_round=round(MdssInfo['perc'])))    
    print("List of the {} major users, CMS person is responsible to decide who needs to be in the report".format(count))
    print("Perc.\t Usage \t Login \t Name")
    db1.print_gdata_perc('\t', count, GdataInfo['grant'], date=date)

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--project', '-P', default=[os.environ["PROJECT"]],nargs='*')
    parser.add_argument('--period', help="Time period in year.quarter (e.g. 2015.q4)")
    parser.add_argument('--count', default=10, type=int, help='number of users to calculate usage for')


    args = parser.parse_args()

    if args.period is not None:
        year, quarter = args.period.split(".")
    else:
        date = datetime.datetime.now()
        year, quarter = datetoyearquarter(date)

    # For usage info:
    date = datetime.date.today() - datetime.timedelta(days=2)

    for project in args.project:

        # Open db
        dbfileprefix = '/short/public/aph502/.data/'
        dbfile = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,year))
#        try:
        db_alloc = ProjectInfo(project,dbfile)
#        except:
#            print("ERROR! You are not a member of this group: ",project)
#            continue
# ------
# Overall project info:
#-------
        # Gdata
        GdataInfo = db_alloc.get_GdataInfo(year, quarter, dbfile, date=date)
        # mdss
        MdssInfo = db_alloc.get_MdssInfo(year, quarter)

#-------
# Main users info:
#-------
# Print the 'count' major users (def. 10), CMS person is responsible to decide who need to be in the report

        db1=UserInfo(project,year)

        output_info(GdataInfo, MdssInfo, db1, date, args.count, project)


if __name__ == "__main__":
    main()
