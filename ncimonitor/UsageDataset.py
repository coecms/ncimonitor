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

from dataset import connect
import datetime
from pwd import getpwnam
import pandas as pd

class NotInDatabase(Exception):
    pass

class ProjectDataset(object):

    def __init__(self, project, dbfile=None):
        self.project = project
        if dbfile is None:
            dbfile = "usage_{}.db".format(project)
        self.dbfile = dbfile
        self.db = connect(dbfile)

    def adduser(self, username, fullname=None):
        if self.db['User'].find_one(username=username) is None:
            if fullname is None:
                try:
                    fullname = getpwnam(username).pw_gecos
                except KeyError:
                    fullname = username
            data = dict(username=username, fullname=fullname)
            self.db['User'].upsert(data, list(data.keys()))

    def addquarter(self, year, quarter, startdate, enddate):
        data = dict(year=year, quarter=quarter, start_date=startdate, end_date=enddate)
        return self.db['Quarter'].upsert(data, ['year', 'quarter'])

    def addgrant(self, year, quarter, totalgrant):
        data = dict(year=year, quarter=quarter, total_grant=totalgrant)
        return self.db['Grant'].upsert(data, ['year', 'quarter'])

    def adduserusage(self, date, username, usecpu, usewall, usesu):
        user = self.db['User'].find_one(username=username)
        data = dict(date=date, user=user['id'], usage_cpu=float(usecpu), usage_wall=float(usewall), usage_su=float(usesu))
        return self.db['UserUsage'].upsert(data, ['date','user'])

    def addsystemqueue(self, systemname, queuename, weight):
        data = dict(system=systemname,queue=queuename,chargeweight=float(weight))
        return self.db['SystemQueue'].upsert(data, ['system', 'queue'])

    def addsystemstorage(self, systemname, storagepoint, year, quarter, grant, igrant):
        data = dict(system=systemname,storagepoint=storagepoint,year=year,quarter=quarter,grant=float(grant),igrant=float(igrant))
        return self.db['SystemStorage'].upsert(data, ['system', 'storagepoint', 'year', 'quarter'])

    def addprojectusage(self, date, systemname, queuename, cputime, walltime, su):
        systemqueue = self.db['SystemQueue'].find_one(system=systemname,queue=queuename)
        data = dict(date=date,systemqueue=systemqueue['id'],usage_cpu=float(cputime),usage_wall=float(walltime),usage_su=float(su))
        return self.db['ProjectUsage'].upsert(data, ['date', 'systemqueue'])

    def addshortusage(self, folder, username, size, inodes, scandate):
        user = self.db['User'].find_one(username=username)
        data = dict(user=user['id'], folder=folder, scandate=scandate, inodes=float(inodes), size=float(size))
        return self.db['ShortUsage'].upsert(data, ['scandate', 'folder', 'user'])

    def addgdatausage(self, storagepoint, folder, username, size, inodes, scandate):
        user = self.db['User'].find_one(username=username)
        data = dict(user=user['id'], storagepoint=storagepoint, folder=folder, scandate=scandate, inodes=float(inodes), size=float(size))
        return self.db['GdataUsage'].upsert(data, ['scandate', 'storagepoint', 'folder', 'user'])

    def getstartend(self, year, quarter, asdate=False):
        q = self.db['Quarter'].find_one(year=year, quarter=quarter)
        if q is None:
            raise NotInDatabase('No entries in database for {}.{}'.format(year,quarter))
        if asdate:
            return self.date2date(q['start_date']),self.date2date(q['end_date'])
        else:
            return q['start_date'],q['end_date']

    def getgrant(self, year, quarter):
        q = self.db['Grant'].find_one(year=year, quarter=quarter)
        if q is None:
            return None
        return float(q['total_grant'])

    def getprojectsu(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT date, SUM(usage_su) AS totsu FROM ProjectUsage WHERE date between '{}' AND '{}' GROUP BY date ORDER BY date".format(startdate,enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        dates = []; usage = []
        for record in q:
            dates.append(self.date2date(record["date"]))
            usage.append(record["totsu"]/1000.)
        return dates, usage

    def getusersu(self, year, quarter, username, scale=None):
        startdate, enddate = self.getstartend(year, quarter)
        user = self.db['User'].find_one(username=username)
        if user is None:
            raise Exception('User {} does not exist in project {}'.format(username,self.project))
        qstring = "SELECT date, SUM(usage_su) AS totsu FROM UserUsage WHERE date between '{}' AND '{}' AND user={} GROUP BY date ORDER BY date".format(startdate,enddate,user['id'])
        q = self.db.query(qstring)
        if q is None:
            return None
        dates = []; usage = []
        if scale is None: scale = 1.
        for record in q:
            dates.append(self.date2date(record["date"]))
            usage.append(record["totsu"]*scale)
        return dates, usage

    def getusershort(self, year, quarter, username):
        startdate, enddate = self.getstartend(year, quarter)
        user = self.db['User'].find_one(username=username)
        qstring = "SELECT scandate, SUM(size) AS totsize FROM ShortUsage WHERE scandate between '{}' AND '{}' AND user={} GROUP BY scandate ORDER BY scandate".format(startdate,enddate,user['id'])
        q = self.db.query(qstring)
        if q is None:
            return None
        dates = []; usage = []
        for record in q:
            dates.append(self.date2date(record["scandate"]))
            usage.append(record["totsize"])
        return dates, usage

    def getusage(self, year, quarter, datafield='usage_su', namefield='user+name'):

        startdate, enddate = self.getstartend(year, quarter)

        if namefield == 'user+name':
            name_sql = 'printf("%s (%s)", User.fullname, User.username)'
        elif namefield == 'user':
            name_sql = 'User.username'
        else:
            raise ValueError('Incorrect value of namefield: {} Valid values are "user+name" or "user"'.format(namefield))

        if datafield not in ('usage_su','usage_wall','usage_cpu'):
            raise ValueError('Incorrect value of datafield: {} Valid values are "usage_su", "usage_wall" or "usage_cpu"'.format(namefield))

        qstring = """SELECT {namefield} as Name, date as Date, SUM({datafield}) AS totsu
        FROM UserUsage
        LEFT JOIN User ON UserUsage.user = User.id 
        WHERE date between \'{start}\' AND \'{end}\' 
        GROUP BY Name, Date 
        ORDER BY Date"""

        # Pivot makes columns of all the individuals, rows are indexed by date
        try:
            df = pd.read_sql_query(qstring.format(namefield=name_sql,
                                                  datafield=datafield,
                                                  start=startdate,
                                                  end=enddate),
                                                  self.db.executable).pivot_table(index='Date',
                                                                                  columns='Name',
                                                                                  fill_value=0)
        except:
            print("No usage data available")
            return None

        # Get rid of the totsize labels in the multiindex
        df.columns = df.columns.get_level_values(1)
        # Convert date index from labels to datetime objects 
        df.index = pd.to_datetime(df.index, format="%Y-%m-%d")

        return df


    def getstorage(self, year, quarter, storagept='short', datafield='size', namefield='user+name'):

        startdate, enddate = self.getstartend(year, quarter)

        if storagept == 'short':
            table = 'ShortUsage'
        elif storagept == 'gdata':
            table = 'GdataUsage'
        else:
            raise ValueError('Incorrect value of storagept: {} Valid values are "short" or "gdata"'.format(storagept))

        if namefield == 'user+name':
            name_sql = 'printf("%s (%s)", User.fullname, User.username)'
        elif namefield == 'user':
            name_sql = 'User.username'
        else:
            raise ValueError('Incorrect value of namefield: {} Valid values are "user+name" or "user"'.format(namefield))

        if datafield not in ('size','inodes'):
            raise ValueError('Incorrect value of datafield: {} Valid values are "inodes" or "size"'.format(namefield))

        qstring = """SELECT {namefield} as Name, scandate as Date, SUM({datafield}) AS totsize 
        FROM {table}
        LEFT JOIN User ON {table}.user = User.id
        WHERE scandate between \'{start}\' AND \'{end}\'
        GROUP BY Name, Date
        ORDER BY Date"""

        # Pivot makes columns of all the individuals, rows are indexed by date
        try:
            df = pd.read_sql_query(qstring.format(namefield=name_sql,datafield=datafield,table=table,start=startdate,end=enddate), self.db.executable).pivot_table(index='Date',columns='Name',fill_value=0)
        except:
            print("No data available for {}".format(storagept))
            return None
            
        # Get rid of the totsize labels in the multiindex
        df.columns = df.columns.get_level_values(1)
        # Convert date index from labels to datetime objects 
        df.index = pd.to_datetime(df.index, format="%Y-%m-%d")

        # Make a new index from the beginning of the quarter
        newidx = pd.date_range(startdate,df.index[-1])

        # Reindex to beginning of quarter in case we're missing values from the beginning of the quarter
        df = df.reindex(newidx, method='backfill')

        return df

    def getusergdata(self, year, quarter, username):
        startdate, enddate = self.getstartend(year, quarter)
        user = self.db['User'].find_one(username=username)
        qstring = "SELECT scandate, SUM(size) AS totsize FROM GdataUsage WHERE scandate between '{}' AND '{}' AND user={} GROUP BY scandate ORDER BY scandate".format(startdate,enddate,user['id'])
        q = self.db.query(qstring)
        if q is None:
            return None
        dates = []; usage = []
        for record in q:
            dates.append(self.date2date(record["scandate"]))
            usage.append(record["totsize"])
        return dates, usage

    def getshortdates(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT scandate FROM ShortUsage WHERE scandate between '{}' AND '{}' GROUP BY scandate ORDER BY scandate".format(startdate,enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        dates = []
        for record in q:
            dates.append(self.date2date(record["scandate"]))
        return dates

    def getshortusers(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT user FROM ShortUsage WHERE scandate between '{}' AND '{}' GROUP BY user ORDER BY SUM(size) desc".format(startdate,enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        users = []
        for record in q:
            users.append(self.db['User'].find_one(id=record["user"])["username"])
        return users

    def getsuusers(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT user, MAX(usage_su) as maxsu FROM UserUsage WHERE date between '{}' AND '{}' GROUP BY user ORDER BY maxsu desc".format(startdate,enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        users = []
        for record in q:
            users.append(self.db['User'].find_one(id=record["user"])["username"])
        return users

    def getuser(self, username=None):
        return self.db['User'].find_one(username=username)

    def getusers(self):
        qstring = "SELECT username FROM User"
        q = self.db.query(qstring)
        for user in q:
            yield user['username']

    def getqueue(self, system, queue):
        return self.db['SystemQueue'].find_one(system=system, queue=queue)

    def date2date(self, datestring):

        if type(datestring) == datetime.date:
            return datestring
        else:
            return datetime.datetime.strptime(datestring, "%Y-%m-%d").date()

    def getstoragepoints(self, system, year, quarter):
        qstring = "SELECT storagepoint FROM SystemStorage WHERE system is '{}' AND year is '{}' AND quarter is '{}' GROUP BY storagepoint".format(system,year,quarter)
        q = self.db.query(qstring)
        if q is None:
            return None
        storagepoints = []
        for record in q:
            storagepoints.append(record["storagepoint"])
        return storagepoints

    def getgdatastoragept(self, year, quarter):
        q = self.db['SystemStorage'].find_one(system='global', year=year, quarter=quarter)
        if q is None:
            return None
        return q["storagepoint"]

    def getsystemstorage(self, systemname, storagepoint, year, quarter):
        if storagepoint == 'gdata':
            # look up which gdata system this project is using. This is dumb, but it works
            point = self.getgdatastoragept(year, quarter)
        else:
            point = storagepoint
        q = self.db['SystemStorage'].find_one(system=systemname, storagepoint=point, year=year, quarter=quarter)
        if q is None:
            return (None,None)
        return float(q['grant']),float(q['igrant'])


    def top_usage(self, year, quarter, storagepoint, measure='size', count=10, scale=1):
        """
        Return the top ``count`` users according to ``measure`` (either 'size'
        or 'inodes') on ``storagepoint`` for ``year`` and ``quarter``

        Returns pandas dataframe (fullname (username), usage)
        """

        if storagepoint not in ['short', 'gdata']:
            raise Exception(f"Unexpected storagepoint '{storagepoint}'")

        if measure not in ['size', 'inodes']:
            raise Exception(f"Unexpected measure '{measure}'")

        # Get the storage for this quarter, grab the last record, which corresponds
        # to the most recent scan date, sort, take the largest count records and
        # divide by scale
        return self.getstorage(year, 
                               quarter, 
                               storagept=storagepoint, 
                               datafield=measure).ix[-1].sort_values(ascending=False).head(count).divide(scale)
