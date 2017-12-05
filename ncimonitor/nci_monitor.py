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

from warnings import warn
import sys
import os
import datetime
import argparse
# import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.dates import MONDAY, DayLocator, WeekdayLocator, MonthLocator, DateFormatter, drange
from matplotlib.dates import AutoDateFormatter, AutoDateLocator, num2date
import matplotlib.patches as mpatches
from matplotlib.colors import ListedColormap
import numpy as np
from numpy import arange
import random
from itertools import cycle, islice

from collections import OrderedDict
import pandas as pd
from getpass import getuser

# from make_usage_db import *
from UsageDataset import *
from DBcommon import *

plt.style.use('ggplot')


# From http://tools.medialab.sciences-po.fr/iwanthue/
iwanthuecolors = [ "#83DFBA", "#87A9C8", "#A1A643", "#D9A730", "#89E32D", "#58C5E4", "#D8CB5A", "#CBC8E9", "#BF7CF1", "#F248E9", "#69EB7F", "#C592D8", "#AB95C1", "#AE9E55", "#99D37B", "#E27FC7", "#7AA1E5", "#C6E145", "#DC67F3", "#4DE447", "#F07041", "#A6DBE5", "#D9C52D", "#5197F5", "#F26392", "#80AA6A", "#ECB7E2", "#69E9A6", "#E4BF6D", "#86B236", "#DB85E0", "#EC7D71", "#E98D27", "#4FA9E2", "#4EB960", "#D8EA70", "#A39DE0", "#C8E899", "#D4905B", "#D7E828", "#61E2DD", "#63ABA9", "#BD94A6", "#F062C8", "#E08793", "#4FB183", "#A0E562", "#52BA3A", "#978DF0", "#DE89B6"]

brewer_qualitative = [
    '#8dd3c7','#ffffb3','#bebada','#fb8072','#80b1d3','#fdb462','#b3de69','#fccde5','#d9d9d9','#bc80bd','#ccebc5','#ffed6f',
    '#a6cee3','#1f78b4','#b2df8a','#33a02c','#fb9a99','#e31a1c','#fdbf6f','#ff7f00','#cab2d6','#6a3d9a','#ffff99','#b15928',
    ] * 5 

# cm = ListedColormap(iwanthuecolors, "myhues")
cm = ListedColormap(brewer_qualitative, "myhues")

def getidealdates(start, end, deltadays=1):
    return drange(start, end, datetime.timedelta(days=deltadays))

def get_ideal_SU_usage(db, year, quarter, total_grant):
    startdate, enddate = db.getstartend(year, quarter, asdate=True)
    dates = getidealdates(startdate, enddate, 1)
    usage = arange(len(dates))*(total_grant/len(dates))

    return dates, usage

def select_users(df,users):

    # Make a series out of the column names, convert to a string and do a simple "OR" regex
    # as the column name has full name as well as username, and this is taking usernames to match
    df = df.loc[:,df.columns.to_series().str.contains('|'.join(users))]

    if df.shape[1] == 0:
        return None
    else:
        return df 

def sort_table_by_last_row(df):
    df.sort_values(df.last_valid_index(), axis=1, inplace=True, ascending=False)

def plot_storage(db,storagept,year,quarter,datafield,showtotal,cutoff=0,users=None,pdf=False):

    if datafield == 'size':
        # Scale sizes to GB
        # scale = 1.e12       # 1 GB 1000^4
        scale = 1099511627776. # 1 GB 1024^4
        ylabel = "Storage Used (TB)"
    else:
        scale = 1
        ylabel = "Inodes"

    if storagept == 'gdata':
        system = 'global'
    else:
        system = 'raijin'

    dp = db.getstorage(year, quarter, storagept=storagept, datafield=datafield)

    if dp is not None:
        dp = dp / scale
    else:
        return

    if users is not None: dp = select_users(dp,users)

    if dp is None:
        warn("No data to display for this selection")
        return

    ideal = None; sort = True
    if args.delta:
        # Normalise by usage at beginning of the month
        dp = dp - dp.iloc[0,:].values
        # Select columns based on proscribed cutoff
        dp = dp.loc[:,dp.abs().max(axis=0)>cutoff]
        title = "Change in {} file usage since beginning of quarter {}.{} for Project {}".format(storagept,year,quarter,project)
        type = 'line'
    else:
        # Sort now, as sorting is turned off so we keep remainder at top of the plot
        sort_table_by_last_row(dp)
        # Select columns based on proscribed cutoff
        mask = dp.max(axis=0)>cutoff
        if not all(mask):
            othername = 'Remainder'
            # Sort rows by the value of the last row in each column. Only works with recent versions of pandas.
            # Need to sort now as we have an others column we want to retain in place
            dp = pd.concat( [ dp.loc[:,mask], dp.loc[:,~mask].sum(axis=1).rename(othername) ], axis=1)
            sort = False
        title = "{} file usage for Project {} ({}.{})".format(storagept,project,year,quarter)
        type = 'area'
        if showtotal:
            grant, igrant = db.getsystemstorage(system, storagept, year, quarter)
            if datafield == 'size':
                ideal = grant
            else:
                ideal = igrant
            ideal = [ideal/scale] * 2

    outfile = None
    if pdf:
        outfile = "nci_{storagept}_{field}_{proj}_{y}.{q}.pdf".format(storagept=storagept,field=datafield,proj=project,y=year,q=quarter)
      
    plot_dataframe(dp, type=type, ylabel=ylabel, title=title, cutoff=cutoff, ideal=ideal, outfile=outfile, sort=sort, delta=args.delta)

def plot_usage(db,year,quarter,byuser,total,users,pdf=False):

    dp = db.getusage(year, quarter)

    scale = 1000.

    if dp is None:
        return
    else:
        dp = dp / scale

    title = "Usage for Project {} on {} ({}.{})".format(project,system,year,quarter)
    ylabel = "Compute resources (KSU)"

    if users is not None:
        byuser = True
        dp = select_users(dp,users)

    if dp is None:
        warn("No data to display for this selection")
        return

    if not byuser:
        # Sum all the individual users
        dp = dp.sum(axis=1)

    ideal = None
    if total is not None:
        # Fill in the remainder of the quarter with NA so the plot will span entire quarter. The
        # ideal value will then be plotted at the end of the quarter
        dp = dp.reindex(pd.date_range(*db.getstartend(year, quarter, asdate=True)),method='backfill')
        ideal = (0,total)

    outfile = None
    if pdf:
        outfile = "nci_usage_{proj}_{y}.{q}.pdf".format(proj=project,y=year,q=quarter)
      
    plot_dataframe(dp, type='line', ylabel=ylabel, title=title, ideal=ideal, outfile=outfile, legend=byuser)

def plot_dataframe(df, type='line', xlabel=None, ylabel=None, title=None, cutoff=None, ideal=None, outfile=None, legend=True, sort=True, delta=False):

    if any(d == 0 for d in df.shape):
        print("No data to plot")
        return
    
    if len(df.shape) > 1:
        # Sort rows by the value of the last row in each column. Only works with recent versions of pandas.
        if sort: sort_table_by_last_row(df)
        # Make a custom colormap which is just the number of colours we need. Prevents
        # unwanted interpolation
        cm = ListedColormap(brewer_qualitative[:df.shape[1]], "myhues")
    else:
        cm = ListedColormap(brewer_qualitative[:1], "myhues")

    fig = plt.figure(figsize=figsize)

    ax = fig.add_axes([0.1, 0.15, 0.7, 0.7, ])

    if title is not None: ax.set_title(title)
    if xlabel is not None: ax.set_xlabel(xlabel)
    if ylabel is not None: ax.set_ylabel(ylabel)

    if type == 'area':
        df.plot.area(ax=ax,use_index=True, colormap=cm) #, legend='reverse')#.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        if legend:
            handles, labels = ax.get_legend_handles_labels()
            ax.legend(reversed(handles), reversed(labels), loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')
    else:
        df.plot(ax=ax,use_index=True, colormap=cm) #, legend=legend) #.legend(loc='center left', bbox_to_anchor=(1, 0.5))
        if legend: ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')

    if ideal is not None:
        # Plot a blue dashed line to indicate some ideal value or limit
        ax.plot(ax.get_xlim(), ideal, '--', color='blue')

    # Make sure y axis is always updated as we're overlaying new data
    plt.autoscale(enable=True,axis='y')
    if not delta:
        # Always snap bottom axis to zero, but not for --delta so keep in this block
        ax.set_ylim(bottom=0.)

    monthsFmt = DateFormatter("%-d '%b")
    ax.xaxis.set_major_formatter(monthsFmt)

    xtick_locator = AutoDateLocator()
    xtick_formatter = AutoDateFormatter(xtick_locator)
    ax.xaxis.set_major_locator(xtick_locator)

    fig.autofmt_xdate()

    if outfile is not None:
        fig.savefig(outfile)


def main():

    username = getuser()

    parser = argparse.ArgumentParser(description="Show NCI account usage information with more context")

    parser.add_argument("-u","--users", help="Limit information to specified users", action='append')
    parser.add_argument("-p","--period", help="Time period in year.quarter (e.g. 2015.q4)")
    parser.add_argument("-P","--project", help="Specify project id(s)", default=[os.environ["PROJECT"]], nargs='*')
    parser.add_argument("-S","--system", help="System name", default="raijin")
    parser.add_argument("--usage", help="Show SU usage (default true)", action='store_true')
    parser.add_argument("--short", help="Show short usage (default true)", action='store_true')
    parser.add_argument("--gdata", help="Show gdata usage (default true)", action='store_true')
    parser.add_argument("--inodes", help="Show inode usage (default false)", action='store_true')
    parser.add_argument("--byuser", help="Show SU usage by user", action='store_true')
    parser.add_argument("--maxusage", help="Set the maximum SU usage (useful for individual users)", type=float)
    parser.add_argument("--pdf", help="Save pdf copies of plots", action='store_true')
    parser.add_argument("--noshow", help="Do not show plots", action='store_true')
    parser.add_argument("--username", help="Show username rather than full name in plot legend", action='store_true')
    parser.add_argument("-n","--num", help="Show only top num users where appropriate", type=int, default=None)
    parser.add_argument("-c","--cutoff", help="Show only users whose storage exceeds cutoff", type=float, default=None)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--showtotal", help="Show the file usage limit", action='store_true')
    group.add_argument("-d","--delta", help="Show change in file system usage since beginning of time period", action='store_true')


    args = parser.parse_args()
    plot_by_user = False

    figsize=(12,10)

    # If we don't define any of short, usage, or gdata default to all being true
    if not(args.short or args.usage or args.gdata):
        args.usage = True
        args.short = True
        args.gdata = True

    if args.cutoff is not None:
        cutoff = args.cutoff
    else:
        cutoff = 0.
            
    plot_by_user = args.byuser

    if args.period is not None:
        year, quarter = args.period.split(".")
    else:
        date = datetime.datetime.now()
        year, quarter = datetoyearquarter(date)

    if args.inodes:
        datafield = 'inodes'
    else:
        datafield = 'size'
        
    use_full_name = not args.username

    num_show = args.num
    if num_show is not None and num_show < 1: 
        raise ValueError('num must be > 0') 

    dbfileprefix = '/short/public/aph502/.data/'

    for project in args.project:

        dbfile = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,year))
        try:
            db = ProjectDataset(project,dbfile)
        except:
            print("ERROR! You are not a member of this group: ",project)
            continue
        else:

            users = None
            if args.users is not None:
                plot_by_user = True
                users = args.users

            if args.maxusage:
                total_grant = args.maxusage
            else:
                if plot_by_user:
                    # Doesn't make sense to show "ideal" usage when showing individual usage
                    total_grant = None
                else:
                    total_grant = db.getgrant(year, quarter)

            system = args.system
    
            if args.usage:
    
                plot_usage(db,year,quarter,plot_by_user,total_grant,users,args.pdf)
    
            if args.short:
    
                plot_storage(db,'short',year,quarter,datafield,args.showtotal,cutoff,users,args.pdf)
    
            if args.gdata:
    
                plot_storage(db,'gdata',year,quarter,datafield,args.showtotal,cutoff,users,args.pdf)
    
            if not args.noshow: plt.show()

if __name__ == "__main__":
    main()
