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

    def __init__(self, project=None, dburl=None):
        if project is not None:
            self.project = project
            if dburl is None:
                dburl = "usage_{}.db".format(project)
        self.dburl = dburl
        self.db = connect(dburl)

    def adduser(self, user, fullname=None):
        """
        Add a unique user if it doesn't already exist. 
        Return a unique id
        """
        q = self.db['Users'].find_one(user=user)
        if q is None:
            if fullname is None:
                try:
                    fullname = getpwnam(user).pw_gecos
                except KeyError:
                    fullname = user
            data = dict(user=user, fullname=fullname)
            id = self.db['Users'].insert(data, list(data.keys()))
        else:
            id = q['id']
        return id

    def addproject(self, project, description=None):
        """
        Add a unique project code if it doesn't already exist. 
        Return a unique id
        """
        q = self.db['Projects'].find_one(project=project)
        if q is None:
            if description is None:
                description = ''
            data = dict(project=project, description=description)
            id = self.db['Projects'].insert(data, ['project'])
        else:
            id = q['id']
        return id

    def addquarter(self, year, quarter, startdate=None, enddate=None):
        """
        Add a unique quarter
        Return a unique id
        """
        q = self.db['Quarters'].find_one(year=year,quarter=quarter)
        if q is None:
            if startdate is None or enddate is None:
                raise ValueError('Cannot define a new quarter without start and end dates')
            data = dict(year=year, quarter=quarter, start_date=startdate, end_date=enddate)
            id = self.db['Quarters'].insert(data, ['year', 'quarter'])
        else:
            id = q['id']
        return id

    def addsystem(self, system):
        """
        Add a unique system if one doesn't already exist
        Return a unique id
        """
        q = self.db['Systems'].find_one(system=system)
        if q is None:
            data = dict(system=system)
            id = self.db['Systems'].insert(data, ['system'])
        else:
            id = q['id']
        return id

    def addstoragepoint(self, system, storagepoint):
        """
        Add a unique system if one doesn't already exist
        Return a unique id
        """
        system_id = self.addsystem(system)
        q = self.db['StoragePoints'].find_one(system=system_id, storagepoint=storagepoint)
        if q is None:
            data = dict(system=system_id, storagepoint=storagepoint)
            id = self.db['StoragePoints'].insert(data, ['system', 'storagepoint'])
        else:
            id = q['id']
        return id

    def addscheme(self, scheme):
        """
        Add a unique schemeif one doesn't already exist
        Return a unique id
        """
        q = self.db['Schemes'].find_one(scheme=scheme)
        if q is None:
            data = dict(scheme=scheme)
            id = self.db['Schemes'].insert(data, ['scheme'])
        else:
            id = q['id']
        return id

    def addsystemqueue(self, system, queue, weight=None):
        """
        Add a unique system queue if one doesn't already exist
        Return a unique id
        """
        system_id = self.addsystem(system)
        q = self.db['SystemQueues'].find_one(system_id=system_id, queue=queue)
        if q is None:
            if weight is None:
                raise ValueError('Cannot define a new system queue without a value for weight')
            data = dict(system_id=system_id, queue=queue, chargeweight=float(weight))
            id = self.db['SystemQueues'].insert(data, ['system_id', 'queue'])
        else:
            id = q['id']
        return id

    def addusagegrant(self, project, system, scheme, year, quarter, date, allocation):
        """
        Grant is from a scheme for each project. It is per system and quarter,
        but allow (and track) changes to the grant by allowing more than one
        entry per quarter
        """
        project_id = self.addproject(project)
        system_id = self.addsystem(system)
        scheme_id = self.addscheme(scheme)
        quarter_id = self.addquarter(year, quarter)
        data = dict(project_id=project_id, 
                    system_id=system_id, 
                    scheme_id=scheme_id, 
                    quarter_id=quarter_id)
        q = list(self.db['UsageGrants'].find(**data))
        # Only update if there is a change to grant or no grant already defined
        if not q or q[-1]['allocation'] != allocation:
            data = dict(project_id=project_id, 
                        system_id=system_id, 
                        scheme_id=scheme_id, 
                        quarter_id=quarter_id, 
                        date=date, 
                        allocation=allocation)
            id = self.db['UsageGrants'].insert(data, ['project_id', 'system_id', 'scheme_id', 'quarter_id', 'date'])
        else:
            id = q[-1]['id']
        return id

    def addstoragegrant(self, project, system, storagepoint, scheme, year, quarter, date, granttype, grant):
        """
        Grant is from a scheme for each project. It is per system and quarter,
        but allow (and track) changes to the grant by allowing more than one
        entry per quarter
        """
        project_id = self.addproject(project)
        system_id = self.addsystem(system)
        storagepoint_id = self.addstoragepoint(system, storagepoint)
        scheme_id = self.addscheme(scheme)
        quarter_id = self.addquarter(year, quarter)
        data = dict(project_id=project_id, 
                    system_id=system_id, 
                    storagepoint_id=storagepoint_id, 
                    scheme_id=scheme_id, 
                    quarter_id=quarter_id)
        q = list(self.db['StorageGrants'].find(**data))
        # Only update if there is a change to grant or no grant already defined
        if not q or q[-1][granttype] != grant:
            data = dict(project_id=project_id, 
                        system_id=system_id, 
                        storagepoint_id=storagepoint_id, 
                        scheme_id=scheme_id, 
                        quarter_id=quarter_id, 
                        date=date, 
                        granttype=grant)
            id = self.db['StorageGrants'].insert(data, ['project_id', 'system_id', 'storagepoint_id', 'scheme_id', 'quarter_id', 'date'])
        else:
            id = q[-1]['id']
        return id

    def addprojectusage(self, project, system, queue, date, cputime, walltime, su):
        """
        Add a project usage entry by system and queue
        """
        project_id = self.addproject(project)
        systemqueue_id = self.addsystemqueue(system, queue)
        data = dict(project_id=project_id,
                    systemqueue_id=systemqueue_id,
                    date=date,
                    usage_cpu=float(cputime),
                    usage_wall=float(walltime),
                    usage_su=float(su))
        return self.db['ProjectUsage'].upsert(data, ['project_id', 'systemqueue_id', 'date'])

    def addprojectstorage(self, project, system, storagepoint, date, grant, igrant):
        """
        Add a project storage entry by system and storage point
        """
        project_id = self.addproject(project)
        system_id = self.addsystem(system)
        storagepoint_id = self.addstoragepoint(system, storagepoint)
        data = dict(project=project_id,
                    system_id=system_id,
                    storagepoint_id=storagepoint_id,
                    date=date,
                    grant=float(grant),
                    igrant=float(igrant))
        return self.db['ProjectStorage'].upsert(data, ['project_id', 'system_id', 'storagepoint_id', 'date'])

    def adduserusage(self, project, user, date, usecpu, usewall, usesu, efficiency):
        """
        Add user su usage record by project
        """
        project_id = self.addproject(project)
        user_id = self.adduser(user)
        data = dict(project_id=project_id, 
                    user_id=user_id, 
                    date=date, 
                    usage_cpu=float(usecpu), 
                    usage_wall=float(usewall), 
                    usage_su=float(usesu),
                    efficiency=float(efficiency))
        return self.db['UserUsage'].upsert(data, ['project_id', 'user_id', 'date'])

    def adduserstorage(self, project, user, system, storagepoint, scandate, folder, size, inodes):
        """
        Add user storage usage record by project and storage point
        """
        project_id = self.addproject(project)
        user_id = self.adduser(user)
        storagepoint_id = self.addstoragepoint(system, storagepoint)
        data = dict(project_id=project_id, 
                    user_id=user_id, 
                    storagepoint_id=storagepoint_id,
                    folder=folder, 
                    scandate=scandate, 
                    inodes=float(inodes), 
                    size=float(size))
        return self.db['UserStorage'].upsert(data, ['project_id', 'user_id', 'storagepoint_id', 'folder', 'scandate'])

    def getstartend(self, year, quarter, asdate=False):
        q = self.db['Quarter'].find_one(year=year, quarter=quarter)
        if q is None:
            raise NotInDatabase('No entries in database for {}.{}'.format(year,quarter))
        if asdate:
            return self.date2date(q['start_date']),self.date2date(q['end_date'])
        else:
            return q['start_date'],q['end_date']

    def getgrant(self, year, quarter):
        quarter_id = self.addquarter(year, quarter)
        q = self.db['Grant'].find_one(quarter=quarter_id)
        if q is None:
            return None
        return float(q['total_grant'])

    def getprojectsu(self, project, year, quarter):
        project_id = self.addproject(project)
        startdate, enddate = self.getstartend(year, quarter)
        qstring = """SELECT date, SUM(usage_su) AS totsu FROM ProjectUsage 
                     WHERE project={project} AND 
                     date between '{start}' AND '{end}' 
                     GROUP BY date ORDER BY date
                     """.format(project=project_id, start=startdate, end=enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        dates = []; usage = []
        for record in q:
            dates.append(self.date2date(record["date"]))
            usage.append(record["totsu"]/1000.)
        return dates, usage

    def getusersu(self, project, year, quarter, username, scale=None):
        project_id = self.addproject(project)
        startdate, enddate = self.getstartend(year, quarter)
        user_id = self.adduser(username)
        if user is None:
            raise Exception('User {} does not exist in project {}'.format(username, project))
        qstring = """SELECT date, SUM(usage_su) AS totsu FROM UserUsage 
                     WHERE project={project} AND 
                     date between '{start}' AND '{end}' AND 
                     user={userid} GROUP BY date ORDER BY date
                     """.format(project=project_id, start=startdate, end=enddate, user=user_id)
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
        project_id = self.addproject(project)
        startdate, enddate = self.getstartend(year, quarter)
        user_id = self.adduser(username)
        qstring = """SELECT scandate, SUM(size) AS totsize FROM ShortUsage 
                     WHERE project={project} AND 
                     scandate between '{start}' AND '{end}' AND 
                     user={user} GROUP BY scandate ORDER BY scandate
                     """.format(project=project_id, start=startdate, end=enddate, user=user_id)
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
            name_sql = 'printf("%s (%s)", Users.fullname, Users.username)'
        elif namefield == 'user':
            name_sql = 'Users.username'
        else:
            raise ValueError('Incorrect value of namefield: {} Valid values are "user+name" or "user"'.format(namefield))

        if datafield not in ('usage_su','usage_wall','usage_cpu'):
            raise ValueError('Incorrect value of datafield: {} Valid values are "usage_su", "usage_wall" or "usage_cpu"'.format(namefield))

        qstring = """SELECT {namefield} as Name, date as Date, SUM({datafield}) AS totsu
        FROM UserUsage
        LEFT JOIN Users ON UserUsage.user = Users.id 
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
            name_sql = 'printf("%s (%s)", Users.fullname, Users.username)'
        elif namefield == 'user':
            name_sql = 'Users.username'
        else:
            raise ValueError('Incorrect value of namefield: {} Valid values are "user+name" or "user"'.format(namefield))

        if datafield not in ('size','inodes'):
            raise ValueError('Incorrect value of datafield: {} Valid values are "inodes" or "size"'.format(namefield))

        qstring = """SELECT {namefield} as Name, scandate as Date, SUM({datafield}) AS totsize 
        FROM {table}
        LEFT JOIN Users ON {table}.user = Users.id
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
        user = self.db['Users'].find_one(username=username)
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
            users.append(self.db['Users'].find_one(id=record["user"])["username"])
        return users

    def getsuusers(self, year, quarter):
        startdate, enddate = self.getstartend(year, quarter)
        qstring = "SELECT user, MAX(usage_su) as maxsu FROM UserUsage WHERE date between '{}' AND '{}' GROUP BY user ORDER BY maxsu desc".format(startdate,enddate)
        q = self.db.query(qstring)
        if q is None:
            return None
        users = []
        for record in q:
            users.append(self.db['Users'].find_one(id=record["user"])["username"])
        return users

    def getuser(self, username=None):
        return self.db['Users'].find_one(username=username)

    def getusers(self):
        qstring = "SELECT username FROM Users"
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
