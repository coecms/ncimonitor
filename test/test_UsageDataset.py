#!/usr/bin/env python

from __future__ import print_function

import pytest
import sys

from numpy.testing import assert_array_equal, assert_array_almost_equal
from numpy import arange

import os

# Find the python libraries we're testing
sys.path.append('..')
sys.path.append('.')
from UsageDataset import *
# from nci_monitor import *
from DBcommon import datetoyearquarter

import datetime

dbfileprefix = '.'

def silentrm(filename):
    try:
        os.remove(filename)
    except OSError:
        pass

@pytest.fixture(scope='session')
def db():
    project = 'xx00'
    dbfile = "usage_{}.db".format(project)
    print(dbfile)
    silentrm(dbfile)
    dbfile = "sqlite:///"+dbfile
    return ProjectDataset(project,dbfile)

def test_silentrm():
    # Call it twice in case it it existed first time
    silentrm('bogus_very_unlikely_to_exist_file')
    silentrm('bogus_very_unlikely_to_exist_file')

def test_adduser(db):
    user = 'wxs1984'; fullname = 'Winston Smith'
    db.adduser(user, fullname)
    record = db.getuser(user)
    assert( record['username'] == user )
    assert( record['fullname'] == fullname )

    user = 'bxb1984'; fullname = 'Big Brother'
    db.adduser(user, fullname)
    record = db.getuser(user)
    assert( record['username'] == user )
    assert( record['fullname'] == fullname )

    # Adding a user a second time will not overwrite original
    user = 'bxb1984'; fullname_wrong = 'Big Mother'
    db.adduser(user, fullname_wrong)
    record = db.getuser(user)
    assert( record['username'] == user )
    assert( not record['fullname'] == fullname_wrong )
    assert( record['fullname'] == fullname )

    # Adding a user without an explicit fullname will trigger a
    # lookup, which will fall back on the username
    user = 'xxx1984'
    db.adduser(user)
    record = db.getuser(user)
    assert( record['username'] == user )
    assert( record['fullname'] == user )

def test_addquarter(db):
    year = 1984; month = 7; day = 1 
    startdate = datetime.date(year, month, day)
    year = 1984; month = 9; day = 30 
    enddate = datetime.date(year, month, day)
    quarter = 'q3'
    db.addquarter(year,quarter,startdate,enddate)
    sdate, edate = db.getstartend(year, quarter, asdate=True)
    assert( sdate == startdate )
    assert( edate == enddate )

    year = 1984; month = 10; day = 1 
    startdate = datetime.date(year, month, day)
    year = 1984; month = 12; day = 31 
    enddate = datetime.date(year, month, day)
    quarter = 'q4'
    db.addquarter(year,quarter,startdate,enddate)
    sdate, edate = db.getstartend(year, quarter, asdate=True)
    assert( sdate == startdate )
    assert( edate == enddate )

def test_addgrant(db):
    year = 1984; quarter = 'q3'
    grant = 5000000
    db.addgrant(year,quarter,grant)
    assert( db.getgrant(year, quarter) == grant )

    # Second invocation of addgrant should replace the current value
    grant = 1000000
    db.addgrant(year,quarter,grant)
    assert( db.getgrant(year, quarter) == grant )

    year = 1984; quarter = 'q4'
    grant = 5000000
    db.addgrant(year,quarter,grant)
    assert( db.getgrant(year, quarter) == grant )

def test_systemqueue(db):
    system = 'deepblue'
    queue = 'normal'
    weight = 5.
    db.addsystemqueue(system,queue,weight)
    assert( db.getqueue(system, queue)['id'] == 1 )

def test_addsystemstorage(db):
    year = 1984;
    system = 'deepblue'
    storagepoints = ['data', 'short', 'tape']
    grants = [1e6, 2e5, 3e9]
    igrants = [1e7, 2e5, 3e10]
    quarters = ['q3','q4']
    for quarter in quarters:
        for point, grant, igrant in zip(storagepoints, grants, igrants):
            print(point, quarter, grant, igrant)
            db.addsystemstorage(system, point, year, quarter, grant, igrant)

    for i, point in enumerate(storagepoints):
        for quarter in quarters:
            grant, igrant = db.getsystemstorage(system, point, year, quarter)
            print(i, quarter, grant, igrant)
            assert( grant == grants[i] )
            assert( igrant == igrants[i] )
        
def test_adduserusage(db):
    year = 1984; quarter = 'q3'
    startdate, enddate = db.getstartend(year, quarter, asdate=True)
    date = startdate
    usecpu = 0.0; usewall=0.0; usesu = 0.0
    while True:
        for user in db.getusers():
            if user == "xxx1984": continue
            # print(date, user, usecpu, usewall, usesu)
            db.adduserusage(date,user,usecpu,usewall,usesu)
        date = date + datetime.timedelta(days=1)
        if date >= enddate: break
        usecpu += 100.0; usewall += 200.0; usesu += 300.0

    # Grab all users with SU usage > 0 for this quarter
    for user in db.getsuusers(year, quarter):
        # Extract SUs and check sum is the same as 
        dates, sus = db.getusersu(year, quarter, user)
        assert_array_almost_equal (sus, arange(0.0,usesu+300.,300.0) )
        dates, sus = db.getusersu(year, quarter, user, 0.001)
        assert_array_almost_equal (sus, arange(0.0,(usesu+300.)/1000.,0.3) )

def test_addgdatausage(db):
    year = 1984; quarter = 'q3'
    startdate, enddate = db.getstartend(year, quarter, asdate=True)
    date = startdate
    size = 0.; inodes = 0.
    storagept = 'array1'
    while True:
        for user in db.getusers():
            if user == "xxx1984": continue
            folder = 'increasing'
            db.addgdatausage(storagept, folder, user, size, inodes, date)
            folder = 'constant'
            db.addgdatausage(storagept, folder, user, 1000000., 15, date)
        date = date + datetime.timedelta(days=1)
        if date >= enddate: break
        size += 10000.; inodes += 100.

def test_getstoragepoints(db):
    system = 'deepblue'
    year = 1984; quarter = 'q3'
    startdate, enddate = db.getstartend(year, quarter, asdate=True)
    date = startdate
    storagepts = db.getstoragepoints(system, year, quarter)
    print(storagepts)
    
