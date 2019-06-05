#!/usr/bin/env python

"""
Copyright 2019 ARC Centre of Excellence for Climate Extremes

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
import sqlalchemy

class NotInDatabase(Exception):
    pass

class JobsDataset(object):

    def __init__(self, dbfile=None):
        if dbfile is None:
            dbfile = 'sqlite:///jobs.db'
        self.dbfile = dbfile
        self.db = connect(dbfile)

    def getnumrecords(self):
        q = None
        try:
            qstring = 'SELECT count(*) as count FROM Jobs'
            q = self.db.query(qstring)
        except sqlalchemy.exc.OperationalError:
            pass
        if q is None:
            return 0
        for record in q:
            return record['count']

    def addproject(self, project):
        data = dict(project=project)
        return self.db['Project'].upsert(data, list(data.keys()))

    def addqueue(self, queuename):
        data = dict(queue=queuename)
        return self.db['Queue'].upsert(data, list(data.keys()))

    def addstate(self, status):
        data = dict(status=status)
        return self.db['JobState'].upsert(data, list(data.keys()))

    def addexe(self, exepath):
        data = dict(path=exepath)
        return self.db['Executable'].upsert(data, list(data.keys()))

    def adduser(self, username, fullname=None):
        if self.db['User'].find_one(username=username) is None:
            if fullname is None:
                try:
                    fullname = getpwnam(username).pw_gecos
                except KeyError:
                    fullname = username
            data = dict(username=username, fullname=fullname)
            self.db['User'].upsert(data, list(data.keys()))

    def addjob(self, year, queuename, jobid, project, username,
               status, jobname, jobprio, exe, arguments,
               ctime, mtime, qtime, stime, waitime,
               maxwalltime, maxmem, ncpus,
               walltime, mem, cputime, cpuutil, exitstatus):

        self.adduser(username)
        self.addqueue(queuename)
        self.addproject(project)
        self.addstate(status)
        self.addexe(exe)

        user = self.db['User'].find_one(username=username)
        queue = self.db['Queue'].find_one(queue=queuename)
        proj = self.db['Project'].find_one(project=project)
        stat = self.db['JobState'].find_one(status=status)
        exe = self.db['Executable'].find_one(path=exe)

        data = dict(year=year, 
                    jobid=jobid,
                    project=proj['id'], 
                    queue=queue['id'],
                    user=user['id'], 
                    status=stat['id'], 
                    jobname=jobname,
                    exe=exe['id'],
                    ctime=ctime,
                    mtime=mtime,
                    qtime=qtime,
                    stime=stime,
                    waitime=waitime,
                    maxwalltime=maxwalltime,
                    maxmem=maxmem,
                    ncpus=ncpus,
                    walltime=walltime,
                    mem=mem,
                    cputime=cputime,
                    cpuutil=cpuutil,
                    exitstatus=exitstatus
                    )

        return self.db['Jobs'].upsert(data, ['year','jobid'])

    def getjobs(self, startdate=None, enddate=None):
        """
        Returns most useful fields as a pandas dataframe
        """

        qstring = """SELECT User.username, User.fullname, Project.project, Queue.queue, JobState.status, ctime, jobname, waitime, maxwalltime, 
        maxmem, ncpus, mem, cputime, cpuutil, exitstatus from Jobs
        LEFT JOIN Project ON Jobs.project = Project.id
        LEFT JOIN User ON Jobs.user = User.id
        LEFT JOIN Queue ON Jobs.queue = Queue.id
        LEFT JOIN JobState ON Jobs.status = JobState.id
        """

        # Unless start and end date specified return all records
        if startdate is not None and enddate is not None:
            qstring += """WHERE ctime between \'{start}\' AND \'{end}\'"""

        try:
            df = pd.read_sql_query(qstring.format(start=startdate,end=enddate), self.db.executable)
        except:
            print("No data available")
            return None
            
        return df

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
