#!/usr/bin/env python

from __future__ import print_function

import pytest
import sys
import pandas as pd

from numpy.testing import assert_array_equal, assert_array_almost_equal
from numpy import arange

import os

from ncimonitor.UsageDataset import *
from ncimonitor.DBcommon import datetoyearquarter

import datetime

dbfileprefix = '.'

@pytest.fixture(scope='session')
def db():
    project = 'xx00'
    dbfile = "sqlite:///:memory:"
    # dbfile = "sqlite:///usage.db"
    return ProjectDataset(project,dbfile)

def test_adduser(db):
    user = 'wxs1984'; fullname = 'Winston Smith'
    db.adduser(user, fullname)
    record = db.getuser(user)
    assert( record['user'] == user )
    assert( record['fullname'] == fullname )

    user = 'bxb1984'; fullname = 'Big Brother'
    db.adduser(user, fullname)
    record = db.getuser(user)
    assert( record['user'] == user )
    assert( record['fullname'] == fullname )

    # Adding a user a second time will not overwrite original
    user = 'bxb1984'; fullname_wrong = 'Big Mother'
    db.adduser(user, fullname_wrong)
    record = db.getuser(user)
    assert( record['user'] == user )
    assert( not record['fullname'] == fullname_wrong )
    assert( record['fullname'] == fullname )

    # Adding a user without an explicit fullname will trigger a
    # lookup, which will fall back on the user
    user = 'xxx1984'
    db.adduser(user)
    record = db.getuser(user)
    assert( record['user'] == user )
    assert( record['fullname'] == user )

def test_addquarter(db):
    year = 1984; month = 7; day = 1 
    startdate = datetime.date(year, month, day)
    year = 1984; month = 9; day = 30 
    enddate = datetime.date(year, month, day)
    quarter = 'q3'
    db.addquarter(year,quarter,startdate,enddate)
    sdate, edate = db.getstartend(year, quarter)
    assert( sdate == startdate )
    assert( edate == enddate )

    year = 1984; month = 10; day = 1 
    startdate = datetime.date(year, month, day)
    year = 1984; month = 12; day = 31 
    enddate = datetime.date(year, month, day)
    quarter = 'q4'
    db.addquarter(year,quarter,startdate,enddate)
    sdate, edate = db.getstartend(year, quarter)
    assert( sdate == startdate )
    assert( edate == enddate )

def test_addusagegrant(db):
    year = 1984; quarter = 'q3'
    grant = 5000000; scheme = 'Big grant'
    system = 'deepblue'
    year = 1984; month = 7; day = 1 
    date = datetime.date(year, month, day)
    db.addusagegrant(db.project, system, scheme, year, quarter, date, grant)
    assert( len(list(db.db["UsageGrants"].all())) == 1 )
    assert( db.getusagegrant(db.project, system, scheme, year, quarter) == grant )

    # Add same record, shouldn't do anything
    db.addusagegrant(db.project, system, scheme, year, quarter, date, grant)
    assert( len(list(db.db["UsageGrants"].all())) == 1 )
    assert( db.getusagegrant(db.project, system, scheme, year, quarter) == grant )

    # Second invocation of addgrant should add new record and replace the current 
    # value returned by getusagegrant
    grant = 1000000
    year = 1984; month = 7; day = 3 
    date = datetime.date(year, month, day)
    db.addusagegrant(db.project, system, scheme, year, quarter, date, grant)
    assert( db.getusagegrant(db.project, system, scheme, year, quarter) == grant )

    year = 1984; quarter = 'q4'
    grant = 5000000
    year = 1984; month = 10; day = 1 
    date = datetime.date(year, month, day)
    db.addusagegrant(db.project, system, scheme, year, quarter, date, grant)
    assert( db.getusagegrant(db.project, system, scheme, year, quarter) == grant )

def test_systemqueue(db):
    system = 'deepblue'
    queue = 'normal'
    weight = 5.
    db.addsystemqueue(system, queue, weight)
    assert( db.getqueue(system, queue)['id'] == 1 )

def test_addstoragegrant(db):
    year = 1984;
    system = 'deepblue'
    scheme = 'Big grant'
    storagepoints = ['data', 'short', 'tape']
    grants = [1e6, 2e5, 3e9]
    igrants = [1e7, 2e5, 3e10]
    quarters = ['q3','q4']
    year = 1984; month = 10; day = 1 
    date = datetime.date(year, month, day)
    for quarter in quarters:
        for point, grant in zip(storagepoints, grants):
            print(point, quarter, grant)
            db.addstoragegrant(project=db.project, 
                               system=system, 
                               storagepoint=point, 
                               scheme=scheme,
                               year=year, 
                               quarter=quarter, 
                               date=date,
                               storagetype='capacity',
                               grant=grant)

    for quarter in quarters:
        for point, igrant in zip(storagepoints, igrants):
            print(point, quarter, igrant)
            db.addstoragegrant(project=db.project, 
                               system=system, 
                               storagepoint=point, 
                               scheme=scheme,
                               year=year, 
                               quarter=quarter, 
                               date=date,
                               storagetype='inodes',
                               grant=igrant)

    for i, point in enumerate(storagepoints):
        for quarter in quarters:
            grant, igrant = db.getstoragegrant(db.project, system, point, scheme, year, quarter)
            assert( grant == grants[i] )
            assert( igrant == igrants[i] )
        
def test_adduserusage(db):
    year = 1984; quarter = 'q3'
    startdate, enddate = db.getstartend(year, quarter)
    date = startdate
    usecpu = 0.0; usewall=0.0; usesu = 0.0
    efficiency = 99.8
    while True:
        for user in db.getusers():
            if user == "xxx1984": continue
            # print(date, user, usecpu, usewall, usesu)
            db.adduserusage(db.project, user, date, usecpu, usewall, usesu, efficiency)
        date = date + datetime.timedelta(days=1)
        if date >= enddate: break
        usecpu += 100.0; usewall += 200.0; usesu += 300.0

    # Grab all users with SU usage > 0 for this quarter
    for user in db.getsuusers(year, quarter):
        # Extract SUs and check sum is the same as 
        dates, sus = db.getusersu(db.project, year, quarter, user)
        assert_array_almost_equal (sus, arange(0.0,usesu+300.,300.0) )
        dates, sus = db.getusersu(db.project, year, quarter, user, 0.001)
        assert_array_almost_equal (sus, arange(0.0,(usesu+300.)/1000.,0.3) )

def test_adduserstorage(db):
    system = 'deepblue'
    year = 1984; quarter = 'q3'
    startdate, enddate = db.getstartend(year, quarter)
    date = startdate
    size = 0.; inodes = 0.
    storagept = 'array1'
    while True:
        for user in db.getusers():
            if user == "xxx1984": continue
            folder = 'increasing'
            db.adduserstorage(db.project, user, system, storagept, date, folder, size, inodes)
            folder = 'constant'
            db.adduserstorage(db.project, user, system, storagept, date, folder, 1000000., 15)
        date = date + datetime.timedelta(days=1)
        if date >= enddate: break
        size += 10000.; inodes += 100.

def test_getstorage(db):
    system = 'deepblue'
    year = 1984; quarter = 'q3'
    
    dp = db.getstorage(db.project, year, quarter, storagept='array1', datafield='size')
    assert(dp['Winston Smith (wxs1984)'].sum() == 131950000.0)

    dp = db.getstorage(db.project, year, quarter, storagept='array1', datafield='inodes')
    assert(dp['Big Brother (bxb1984)'].sum() == 410865.0)

def test_getstoragepoints(db):
    system = 'deepblue'
    storagepts = db.getstoragepoints(system)
    assert storagepts == ['data', 'short', 'tape', 'array1']
    
def test_getusage(db):
    system = 'deepblue'
    year = 1984; quarter = 'q3'
    
    dp = db.getusage(year, quarter)
    assert(dp['Winston Smith (wxs1984)'].sum() == 1228500)
    assert(dp['Big Brother (bxb1984)'].sum() == 1228500)

        
