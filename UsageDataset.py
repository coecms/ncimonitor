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

from dataset import *
import datetime
from pwd import getpwnam

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
            self.db['User'].upsert(data, data.keys())

    def addquarter(self, year, quarter, startdate, enddate):
        data = dict(year=year, quarter=quarter, start_date=startdate, end_date=enddate)
        return self.db['Quarter'].upsert(data, ['year', 'quarter'])

    def addgrant(self, year, quarter, totalgrant):
        data = dict(year=year, quarter=quarter, total_grant=totalgrant)
        return self.db['Grant'].upsert(data, ['year', 'quarter'])

    def adduserusage(self, date, username, usecpu, usewall, usesu):
        user = self.db['User'].find_one(username=username)
        data = dict(date=date, user=user['id'], usage_cpu=usecpu, usage_wall=usewall, usage_su=usesu)
        return self.db['UserUsage'].upsert(data, ['date','user'])

    def addsystemqueue(self, systemname, queuename, weight):
        data = dict(system=systemname,queue=queuename,chargeweight=weight)
        return self.db['SystemQueue'].upsert(data, ['system', 'queue'])

    def addsystemstorage(self, systemname, storagepoint, date, grant, igrant):
        data = dict(system=systemname,storagepoint=storagepoint,date=date,grant=grant,igrant=igrant)
        return self.db['SystemStorage'].upsert(data, ['system', 'storagepoint', 'date'])

    def addprojectusage(self, date, systemname, queuename, cputime, walltime, su):
        systemqueue = self.db['SystemQueue'].find_one(system=systemname,queue=queuename)
        data = dict(date=date,systemqueue=systemqueue['id'],usage_cpu=cputime,usage_wall=walltime,usage_su=su)
        return self.db['ProjectUsage'].upsert(data, ['date', 'systemqueue'])

    def addshortusage(self, folder, username, size, inodes, scandate):
        user = self.db['User'].find_one(username=username)
        data = dict(user=user['id'], folder=folder, scandate=scandate, inodes=inodes, size=size)
        return self.db['ShortUsage'].upsert(data, ['scandate', 'folder', 'user'])

    def addgdatausage(self, folder, username, size, inodes, scandate):
        user = self.db['User'].find_one(username=username)
        data = dict(user=user['id'], folder=folder, scandate=scandate, inodes=inodes, size=size)
        return self.db['GdataUsage'].upsert(data, ['scandate', 'folder', 'user'])

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

    def getshortdates(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT DISTINCT scandate FROM ShortUsage WHERE scandate between '{}' AND '{}' GROUP BY scandate ORDER BY scandate".format(startdate,enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        dates = []
        for record in q:
            dates.append(self.date2date(record["scandate"]))
        return dates

    def getshortusers(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT DISTINCT user FROM ShortUsage WHERE scandate between '{}' AND '{}' ORDER BY SUM(size) desc".format(startdate,enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        users = []
        for record in q:
            users.append(self.db['User'].find_one(id=record["user"])["username"])
        return users

    def getsuusers(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT DISTINCT user FROM UserUsage WHERE date between '{}' AND '{}' ORDER BY SUM(usage_su) desc".format(startdate,enddate)
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
        return datetime.datetime.strptime(datestring, "%Y-%m-%d").date()
