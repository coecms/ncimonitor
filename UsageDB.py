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

from peewee import *

class BaseModel(Model):
    class Meta:
        # database = self.db
        database = None

class User(BaseModel):
    username = CharField(unique=True)
    fullname = CharField()

class Project(BaseModel):
    projid = CharField(unique=True)

class Quarter(BaseModel):
    year = CharField()
    quarter = CharField()
    start_date = DateField()
    end_date = DateField()
    class Meta:
        # Combination of year and quarter must be unique
        indexes = ( (('year','quarter'), True), )

class ProjectGrant(BaseModel):
    projid = ForeignKeyField(Project, to_field="projid")
    quarter = ForeignKeyField(Quarter)
    total_grant = FloatField()
    class Meta:
        # Combination of projid+quarter must be unique
        indexes = ( (('projid','quarter'), True), )

class ProjectUser(BaseModel):
    projid = ForeignKeyField(Project, to_field="projid")
    user = ForeignKeyField(User, related_name='projects', to_field="username")
    class Meta:
        # Combination of projid+user must be unique
        indexes = ( (('projid','user'), True), )

class UserUsage(BaseModel):
    # Support times as well as dates to support sub-day timescales if required
    date = DateTimeField()
    projid = ForeignKeyField(Project, to_field="projid")
    user = ForeignKeyField(User, to_field="username")
    usage_cpu = FloatField()
    usage_wall = FloatField()
    usage_su = FloatField()
    class Meta:
        # Combination of date+projid+user must be unique
        indexes = ( (('date','projid','user'), True), )

class System(BaseModel):
    system = CharField(unique=True)

class SystemQueue(BaseModel):
    system = ForeignKeyField(System, to_field="system")
    queue = CharField()
    chargeweight = FloatField()
    class Meta:
        # Combination of system+queue must be unique
        indexes = ( (('system','queue'), True), )

class SystemStorage(BaseModel):
    system = ForeignKeyField(System, to_field="system")
    storagepoint = CharField()
    date = DateTimeField()
    grant = FloatField()
    igrant = FloatField()
    class Meta:
        # Combination of system+storagepoint+date must be unique
        indexes = ( (('system','storagepoint','date'), True), )

class ProjectUsage(BaseModel):
    # Support times as well as dates to support sub-day timescales if required
    date = DateTimeField()
    systemqueue = ForeignKeyField(SystemQueue)
    projid = ForeignKeyField(Project, to_field="projid")
    usage_cpu = FloatField()
    usage_wall = FloatField()
    usage_su = FloatField()
    class Meta:
        # Combination of date+systemqueue+projid must be unique
        indexes = ( (('date','systemqueue','projid'), True), )    

class ShortUsage(BaseModel):
    projid = ForeignKeyField(Project, to_field="projid")
    folder = CharField()
    user = ForeignKeyField(User, to_field="username")
    # Support only dates. Don't want sub-day timescales, makes it harder to join
    # data and snapshots are only taken daily
    scandate = DateField()
    inodes = FloatField()
    size = FloatField()
    class Meta:
        # Combination of projid+folder+user+date must be unique
        indexes = ( (('projid','folder','user','scandate'), True), )

class Session(object):

    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.db = SqliteDatabase(self.dbfile)
        self.tables = []
        self.tables.append(User)
        self.tables.append(UserUsage)
        self.tables.append(Project)
        self.tables.append(ProjectUsage)
        self.tables.append(ShortUsage)
        self.tables.append(ProjectGrant)
        self.tables.append(ProjectUser)
        self.tables.append(Quarter)
        self.tables.append(System)
        self.tables.append(SystemQueue)
        self.tables.append(SystemStorage)

        self.db.connect()
        self.create_tables()

    def create_tables(self):
        # Create the tables.
        self.db.create_tables(self.tables, safe=True)
