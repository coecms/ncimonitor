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

import sys
import os
import datetime
import argparse
# import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.dates import MONDAY, DayLocator, WeekdayLocator, MonthLocator, DateFormatter, drange
from matplotlib.dates import AutoDateFormatter, AutoDateLocator
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

if __name__ == "__main__":

    username = getuser()

    parser = argparse.ArgumentParser(description="Show NCI account usage information with more context")

    parser.add_argument("-u","--users", help="Limit information to specified users", action='append')
    parser.add_argument("-p","--period", help="Time period in year.quarter (e.g. 2015.q4)")
    parser.add_argument("-P","--project", help="Specify project id(s)", default=[os.environ["PROJECT"]], nargs='*')
    parser.add_argument("-S","--system", help="System name", default="raijin")
    parser.add_argument("--usage", help="Show SU usage (default true)", action='store_true')
    parser.add_argument("--short", help="Show short usage (default true)", action='store_true')
    parser.add_argument("--inodes", help="Show inode usage (default false)", action='store_true')
    parser.add_argument("--byuser", help="Show SU usage by user", action='store_true')
    parser.add_argument("--maxusage", help="Set the maximum SU usage (useful for individual users)", type=float)
    parser.add_argument("--pdf", help="Save pdf copies of plots", action='store_true')
    parser.add_argument("--noshow", help="Do not show plots", action='store_true')
    parser.add_argument("--username", help="Show username rather than full name in plot legend", action='store_true')
    parser.add_argument("-n","--num", help="Show only top num users where appropriate", type=int, default=None)
    parser.add_argument("-c","--cutoff", help="Show only users whose usage exceeds cutoff", type=float, default=None)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--shorttotal", help="Show the short file limit", action='store_true')
    group.add_argument("-d","--delta", help="Show change in short usage since beginning of time period", action='store_true')


    args = parser.parse_args()
    plot_by_user = False

    figsize=(12,10)

    # If we define either short or usage, disable the other unless it is explicitly specified
    # and default to both being true
    if args.short:
        if not args.usage:
            args.usage = False
    elif args.usage:
        if not args.short:
            args.short = False
    else:
        args.usage = True
        args.short = True
            
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

    # Currently not implemented
    SU_threshold = 0.0

    dbfileprefix = '/short/public/aph502/.data/'

    for project in args.project:

        users = []
        if args.users is not None:
            plot_by_user = True
            users.extend(args.users)

        dbfile = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,year))
        try:
            db = ProjectDataset(project,dbfile)
        except:
            print("ERROR! You are not a member of this group: ",project)
            continue

        if args.maxusage:
            total_grant = args.maxusage
        else:
            total_grant = db.getgrant(year, quarter)

        system = args.system

        ideal_dates, ideal_usage = get_ideal_SU_usage(db, year, quarter, total_grant)

        if args.usage:
    
            plotted = False
            fig1 = plt.figure(figsize=figsize)

            if (plot_by_user):

                ax = fig1.add_axes([0.1, 0.15, 0.7, 0.7 ])
                ax.set_xlabel("Date")

                if len(users) <= 0:
                    users = db.getsuusers(year, quarter)
                    if num_show is not None:
                        users = users[0:min(num_show,len(users))]

                ucols = zip(users, cycle(iwanthuecolors)) if len(users) > len(iwanthuecolors) else zip(users, iwanthuecolors)
    
                for user, color in ucols:
                    dates, sus = db.getusersu(year, quarter, user, scale=0.001)
                    if len(sus) <= 0: continue 
                    if max(sus) > SU_threshold:
                        plotted = True
                        if use_full_name:
                            namelabel = db.getuser(user)['fullname']
                        else:
                            namelabel = user
                        ax.plot(dates, sus, color=color, linewidth=2, label=namelabel)

                if plotted:
                    if args.maxusage: ax.plot(ideal_dates, ideal_usage, '--', color='blue')
    
                    ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')
                else:
                    print("No usage data found to plot")

            else:

                dates, sus = db.getprojectsu(year, quarter)

                if len(sus) > 0:

                    ax = fig1.add_axes([0.1, 0.15, 0.85, 0.8, ])
                    ax.set_xlabel("Date")

                    ax.plot(ideal_dates, ideal_usage, '--', color='blue')
    
                    ax.plot(dates, sus, color='red')

                    plotted = True

            if (plotted):
                ax.set_title("Usage for Project {} on {} ({}.{})".format(project,system,year,quarter))
                ax.set_ylabel("KSUs")
    
                monthsFmt = DateFormatter("%-d '%b")
                ax.xaxis.set_major_formatter(monthsFmt)
    
                xtick_locator = AutoDateLocator()
                xtick_formatter = AutoDateFormatter(xtick_locator)
                ax.xaxis.set_major_locator(xtick_locator)
    
                fig1.autofmt_xdate()
    
                if args.pdf:
                    outfile = "nci_usage_{}_{}.{}.pdf".format(project,year,quarter)
                    fig1.savefig(outfile)
            else:
                # Left for refernce: these do not work and create an error, so just let an
                # empty plot be created instead
                # plt.clf()
                # plt.close(fig1)
                pass
            

        if args.short:

            fig2 = plt.figure(figsize=figsize)

            ax = fig2.add_axes([0.1, 0.15, 0.7, 0.7, ])

            if datafield == 'size':
                # Scale sizes to GB
                # scale = 1.e12       # 1 GB 1000^4
                scale = 1099511627776. # 1 GB 1024^4
                ylabel = "Storage Used (TB)"
            else:
                scale = 1
                ylabel = "Inodes"

            dp = db.getstorage(year, quarter, storagept='short', datafield=datafield) / scale

            if args.cutoff is None:
                cutoff = 0
            else:
                cutoff = args.cutoff

            if args.delta:
                # Normalise by usage at beginning of the month
                dp = dp - dp.iloc[0,:].values
                # Select columns based on proscribed cutoff
                dp = dp.loc[:,dp.abs().max(axis=0)>cutoff]
                title = "Change in short file usage since beginning of quarter {}.{} for Project {} on {}".format(year,quarter,project,system)
            else:
                # Select columns based on proscribed cutoff
                dp = dp.loc[:,dp.max(axis=0)>cutoff]
                title = "Short file usage for Project {} on {} ({}.{})".format(project,system,year,quarter)

            ax.set_title(title)
            ax.set_ylabel(ylabel)

            # Sort rows by the value of the last row in each column. Only works with recent versions of pandas.
            dp.sort_values(dp.last_valid_index(), axis=1, inplace=True, ascending=False)

            # Make a custom colormap which is just the number of colours we need. Prevents
            # unwanted interpolation
            cm = ListedColormap(brewer_qualitative[:dp.shape[1]], "myhues")

            if (args.delta):
                dp.plot(ax=ax,use_index=True, colormap=cm, legend=True)#.legend(loc='center left', bbox_to_anchor=(1, 0.5))
                ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')
            else:
                dp.plot.area(ax=ax,use_index=True, colormap=cm, legend='reverse')#.legend(loc='center left', bbox_to_anchor=(1, 0.5))
                handles, labels = ax.get_legend_handles_labels()
                ax.legend(reversed(handles), reversed(labels), loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')

            if args.shorttotal:
                grant, igrant = db.getsystemstorage(system, 'short', year, quarter)
                if datafield == 'size':
                    grant = grant/scale
                else:
                    grant = igrant
                # Plot a blue dashed line to indicate the 
                ax.plot(ax.get_xlim(), (grant, grant), '--', color='blue')
                # Make sure y axis is always updated as we're overlaying new data
                plt.autoscale(enable=True,axis='y')
                # Always snap bottom axis to zero, but not for --delta so keep in this block
                ax.set_ylim(bottom=0.)
    
            # monthsFmt = DateFormatter("%-d '%b")
            # ax.xaxis.set_major_formatter(monthsFmt)

            # xtick_locator = AutoDateLocator()
            # xtick_formatter = AutoDateFormatter(xtick_locator)
            # ax.xaxis.set_major_locator(xtick_locator)

            ## fig2.autofmt_xdate()

            if args.pdf:
                outfile = "nci_short_{}_{}.{}.pdf".format(project,year,quarter)
                fig2.savefig(outfile)

    if not args.noshow: plt.show()
