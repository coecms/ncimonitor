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
from ncimonitor.JobsDataset import *
from ncimonitor.DBcommon import *

plt.style.use('ggplot')


# From http://tools.medialab.sciences-po.fr/iwanthue/
iwanthuecolors = [ "#83DFBA", "#87A9C8", "#A1A643", "#D9A730", "#89E32D", "#58C5E4", "#D8CB5A", "#CBC8E9", "#BF7CF1", "#F248E9", "#69EB7F", "#C592D8", "#AB95C1", "#AE9E55", "#99D37B", "#E27FC7", "#7AA1E5", "#C6E145", "#DC67F3", "#4DE447", "#F07041", "#A6DBE5", "#D9C52D", "#5197F5", "#F26392", "#80AA6A", "#ECB7E2", "#69E9A6", "#E4BF6D", "#86B236", "#DB85E0", "#EC7D71", "#E98D27", "#4FA9E2", "#4EB960", "#D8EA70", "#A39DE0", "#C8E899", "#D4905B", "#D7E828", "#61E2DD", "#63ABA9", "#BD94A6", "#F062C8", "#E08793", "#4FB183", "#A0E562", "#52BA3A", "#978DF0", "#DE89B6"]

brewer_qualitative = [
    '#8dd3c7','#ffffb3','#bebada','#fb8072','#80b1d3','#fdb462','#b3de69','#fccde5','#d9d9d9','#bc80bd','#ccebc5','#ffed6f',
    '#a6cee3','#1f78b4','#b2df8a','#33a02c','#fb9a99','#e31a1c','#fdbf6f','#ff7f00','#cab2d6','#6a3d9a','#ffff99','#b15928',
    ] * 5 

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

def plot_usage(db,project,system,year,quarter,byuser,total,users,pdf=False):

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

    figsize=(12,10)
    fig = plt.figure(figsize=figsize)

    ax = fig.add_axes([0.1, 0.15, 0.7, 0.7, ])

    if title is not None: ax.set_title(title)
    if xlabel is not None: ax.set_xlabel(xlabel)
    if ylabel is not None: ax.set_ylabel(ylabel)

    if type == 'area':
        df.plot.area(ax=ax,use_index=True, colormap=cm, xlim=(df.index[0],df.index[-1]))
        if legend:
            handles, labels = ax.get_legend_handles_labels()
            ax.legend(reversed(handles), reversed(labels), loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')
    else:
        df.plot(ax=ax, use_index=True, colormap=cm, xlim=(df.index[0],df.index[-1]))
        if legend: ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')

    if ideal is not None:
        # Plot a blue dashed line to indicate some ideal value or limit
        ax.plot(ax.get_xlim(), ideal, '--', color='blue')

    # Make sure y axis is always updated as we're overlaying new data
    plt.autoscale(enable=True,axis='y')
    if not delta:
        # Always snap bottom axis to zero, but not for --delta so keep in this block
        ax.set_ylim(bottom=0.)

    if outfile is not None:
        fig.savefig(outfile)


def main():

    username = getuser()

    parser = argparse.ArgumentParser(description="Show NCI job usage information")

    parser.add_argument("-db","--database", help="Database file", default='jobs.db')
    parser.add_argument("-u","--users", help="Limit information to specified users", action='append')
    parser.add_argument("-p","--period", help="Time period in year.quarter (e.g. 2015.q4)")
    parser.add_argument("-P","--project", help="Specify project id(s)", nargs='*')
    parser.add_argument("-S","--system", help="System name", default="raijin")
    parser.add_argument("--pdf", help="Save pdf copies of plots", action='store_true')
    parser.add_argument("--noshow", help="Do not show plots", action='store_true')
    parser.add_argument("--username", help="Show username rather than full name in plot legend", action='store_true')
    parser.add_argument("-v","--plotvar", help="Variable to plot", default='waittime')
    parser.add_argument("-g","--groupvar", help="Variable by which to group", default='queue')
    parser.add_argument("-s","--splitvar", help="Variable by which to split groups", default='ncpusbin')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--showtotal", help="Show the file usage limit", action='store_true')
    group.add_argument("-d","--delta", help="Show change in file system usage since beginning of time period", action='store_true')

    args = parser.parse_args()
    plot_by_user = False

    if args.period is not None:
        year, quarter = args.period.split(".")
    else:
        date = datetime.datetime.now()
        year, quarter = datetoyearquarter(date)

    use_full_name = not args.username

    dbfile = 'sqlite:///'+os.path.join(args.database)
    try:
        db = JobsDataset(dbfile)
    except:
        print("ERROR! You are not a member of this group: ",project)
    else:

        df = db.getjobs()

        if df.empty:
            raise ValueError("No data returned for this query")

        if args.project:
            project = []
            for p in args.project:
                if p == 'clex':
                    project.extend(['w35', 'w40', 'w42', 'w48', 'w97', 'v45'])
                elif p == 'mom':
                    project.extend(['v45', 'e14', 'x77', 'g40'])
                else:
                    project.append(p)
            project = set(project)
            print(project)
            df = df.loc[df.project.isin(project),]

        users = None
        if args.users is not None:
            users = args.users
            df = df.loc[df.username.isin(args.users),]

        if df.empty:
            raise ValueError("No data left after applying variable choices")

        # Add a binned job size column using cut
        # pd.cut(df.ncpus,[0,1,2,16,128,1024,float("inf")])

        pd.pivot_table(df, values=args.plotvar, index=args.groupvar, columns=args.splitvar).plot(kind='bar')

        if not args.noshow: plt.show()

if __name__ == "__main__":
    main()
