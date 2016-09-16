#!/usr/bin/env python

import sys
import os
import datetime
import argparse
# import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.dates import MONDAY, DayLocator, WeekdayLocator, MonthLocator, DateFormatter, drange
from matplotlib.dates import AutoDateFormatter, AutoDateLocator
import matplotlib.patches as mpatches
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
iwanthuecolors = [ "#83DFBA", "#87A9C8", "#A1A643", "#D9A730", "#89E32D", "#58C5E4", "#CBC8E9", "#BF7CF1", "#D8CB5A", "#F248E9", "#69EB7F", "#C592D8", "#AB95C1", "#AE9E55", "#99D37B", "#E27FC7", "#7AA1E5", "#C6E145", "#DC67F3", "#4DE447", "#F07041", "#A6DBE5", "#D9C52D", "#5197F5", "#F26392", "#80AA6A", "#ECB7E2", "#69E9A6", "#E4BF6D", "#86B236", "#DB85E0", "#EC7D71", "#E98D27", "#4FA9E2", "#4EB960", "#D8EA70", "#A39DE0", "#C8E899", "#D4905B", "#D7E828", "#61E2DD", "#63ABA9", "#BD94A6", "#F062C8", "#E08793", "#4FB183", "#A0E562", "#52BA3A", "#978DF0", "#DE89B6"]


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
    parser.add_argument("--byuser", help="Show SU usage by user", action='store_true')
    parser.add_argument("--maxusage", help="Set the maximum SU usage (useful for individual users)", type=float)
    parser.add_argument("--pdf", help="Save pdf copies of plots", action='store_true')
    parser.add_argument("--noshow", help="Do not show plots", action='store_true')
    parser.add_argument("--overlay", help="Overlay all plots", action='store_true')
    parser.add_argument("--username", help="Show username rather than full name in plot legend", action='store_true')
    parser.add_argument("-n","--num", help="Show only top num users where appropriate", type=int, default=None)
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

    use_full_name = not args.username

    num_show = args.num
    if num_show is not None and num_show < 1: 
        raise ValueError('num must be > 0') 

    # Currently not implemented
    SU_threshold = 0.0

    dbfileprefix = '/short/public/aph502/.data/'

    overlay = False

    for project in args.project:

        users = []
        if args.users is not None:
            plot_by_user = True
            users.extend(args.users)

        dbfile = 'sqlite:///'+os.path.join(dbfileprefix,"usage_{}_{}.db".format(project,year))
        try:
            db = ProjectDataset(project,dbfile)
        except:
            print "ERROR! You are not a member of this group: ",project
            continue

        if args.maxusage:
            total_grant = args.maxusage
        else:
            total_grant = db.getgrant(year, quarter)

        system = args.system

        ideal_dates, ideal_usage = get_ideal_SU_usage(db, year, quarter, total_grant)

        if args.usage:
    
            if args.overlay and overlay:
                pass
            else:
                fig1 = plt.figure(figsize=figsize)

            if (plot_by_user):

                ax = fig1.add_axes([0.1, 0.15, 0.7, 0.7 ])
                ax.set_xlabel("Date")

                if len(users) <= 0:
                    users = db.getsuusers(year, quarter)
                    if num_show is not None:
                        users = users[0:min(num_show,len(users))]

                ucols = zip(users, cycle(iwanthuecolors)) if len(users) > len(iwanthuecolors) else zip(users, iwanthuecolors)
    
                plotted = False
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
                    print "No usage data found to plot"

            else:

                ax = fig1.add_axes([0.1, 0.15, 0.85, 0.8, ])
                ax.set_xlabel("Date")

                ax.plot(ideal_dates, ideal_usage, '--', color='blue')

                dates, sus = db.getprojectsu(year, quarter)
                ax.plot(dates, sus, color='red')

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

        if args.short:

            if args.overlay and overlay:
                pass
            else:
                fig2 = plt.figure(figsize=figsize)

            ax = fig2.add_axes([0.1, 0.15, 0.7, 0.7, ])
            ax.set_xlabel("Date")

            # Scale sizes to GB
            # scale = 1.e12       # 1 GB 1000^4
            scale = 1099511627776 # 1 GB 1024^4

            usagebyuser = {}

            # Create a set which will contain all unique dates
            date_set = set()

            if not args.users:
                users = db.getshortusers(year, quarter)

            dates = db.getshortdates(year, quarter)
            labels = {}

            for user in users:
                datadates, usage = db.getusershort(year, quarter, user)
                usage = np.array(usage)/scale
                # Use pandas to fill missing values
                # Make a pandas series
                usage = pd.Series(usage, index=datadates)
                # Reindex, and put zeroes in missing locations
                usage = usage.reindex(dates, fill_value=0.)
                if (args.delta):
                    usage = usage - usage[0]
                usagebyuser[user] = usage
                if use_full_name:
                    labels[user] = db.getuser(user)['fullname']
                else:
                    labels[user] = user
                    
            # Sort by the max usage, even if sorted above.
            usagebyuser = OrderedDict(sorted(usagebyuser.items(), key=lambda t: t[1][-1]))

            # Cannot perform user trimming until this point, so logic
            # becomes more convoluted
            if not args.users and num_show is not None:

                if (args.delta):
                    # Pre-sort by the absolute value of max usage
                    # and trim to num_show users
                    usagebyuser = OrderedDict(sorted(usagebyuser.items(), key=lambda t: np.abs(t[1][-1])))

                # Pop off users from the beginning of the list, as it is reverse
                # sorted by size
                while len(usagebyuser) > min(num_show,len(users)):
                    usagebyuser.popitem(last=False)

                if (args.delta):
                    # Sort by the max usage again. Ugly logic, but necessary it seems
                    usagebyuser = OrderedDict(sorted(usagebyuser.items(), key=lambda t: t[1][-1]))

            users = usagebyuser.keys()
            usage_mat = usagebyuser.values()

            # Make an array of colors the same length as users. Recycle colors if
            # necessary
            colors = []
            for user, color in zip(users, cycle(iwanthuecolors)):
                colors.append(color)

            # Flip the colors around, so the first colors are for the highest users, and
            # any repeats are buried in the low users at the bottom
            colors = list(reversed(colors))

            if (args.delta):
                # This is weird. Want to plot the delta's in reverse order so the key has
                # the users in descending order. So reverse the enumeration and then index
                # usage_mat from the end backwards. Hack.
                for i, (user, color) in enumerate(reversed(zip(users,colors))):
                    ax.plot(dates, usage_mat[-1*(i+1)], color=color, linewidth=2, label=labels[user])
                ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), fontsize='small')
            else:
                fields = ax.stackplot(dates, usage_mat, colors=colors, baseline='zero')

                # Reversed the order of the patches to match the order in the stacked plot
                patches = []
                for user, color in zip(reversed(users),reversed(colors)):
                    patches.append(mpatches.Patch(color=color,label=labels[user]))

                # Put a legend to the right of the current axis
                ax.legend(loc='center left', bbox_to_anchor=(1, 0.5),handles=patches, fontsize='small')

            if args.shorttotal:
                grant, igrant = db.getsystemstorage(system, 'short', year, quarter)
                grant = grant/scale
                print grant
                ax.plot(dates, np.ones_like(dates)*grant, '--', color='blue')

                # ax.set_ylim([0,grant-2])
    
            ax.set_title("Short file usage for Project {} on {} ({}.{})".format(project,system,year,quarter))
            ax.set_ylabel("Storage Used (TB)")

            monthsFmt = DateFormatter("%-d '%b")
            ax.xaxis.set_major_formatter(monthsFmt)

            xtick_locator = AutoDateLocator()
            xtick_formatter = AutoDateFormatter(xtick_locator)
            ax.xaxis.set_major_locator(xtick_locator)

            fig2.autofmt_xdate()

            if args.pdf:
                outfile = "nci_short_{}_{}.{}.pdf".format(project,year,quarter)
                fig2.savefig(outfile)

        if args.overlay: overlay = True 

    if not args.noshow: plt.show()


