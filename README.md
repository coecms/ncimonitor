# ncimonitor
Extended usage information for the NCI system raijin.

[![Build Status](https://travis-ci.org/coecms/ncimonitor.svg?branch=master)](https://travis-ci.org/coecms/ncimonitor)

To use:

```bash
module purge
module use ~access/modules
module load ncimonitor
```

Basic usage message:
```bash
ncimonitor -h
usage: ncimonitor [-h] [-u USERS] [-p PERIOD] [-P [PROJECT [PROJECT ...]]]
                  [-S SYSTEM] [--usage] [--short] [--byuser]
                  [--maxusage MAXUSAGE] [--pdf] [--noshow] [-d]

Show NCI account usage information with more context

optional arguments:
  -h, --help            show this help message and exit
  -u USERS, --users USERS
                        Limit information to specified users
  -p PERIOD, --period PERIOD
                        Time period in year.quarter (e.g. 2015.q4)
  -P [PROJECT [PROJECT ...]], --project [PROJECT [PROJECT ...]]
                        Specify project id(s)
  -S SYSTEM, --system SYSTEM
                        System name
  --usage               Show SU usage (default true)
  --short               Show short usage (default true)
  --byuser              Show usage by user
  --maxusage MAXUSAGE   Set the maximum usage (useful for individual users)
  --pdf                 Save pdf copies plots
  --noshow              Do not show plots
  -d, --delta           Show change since beginning of time period
```

Just invoking the program with no options will show a plot of your default project Service Unit (SU) usage and short 
file usage.

You can ask for multiple projects, e.g.
```bash
ncimonitor -P zz55 yy99 qq00
```
If you just want to see just the change in short file usage since the start of the quarter (doesn't affect SU usage):
```bash
ncimonitor --delta
```
Or usage by user:
```bash
ncimonitor --byuser
```
Or just some selected users:
```bash
ncimonitor -u usr1 -u usr2
```
You can change the max usage, which allows you to track individual users against a target
```bash
ncimonitor  -u usr1 -maxusage=1700
```
To save PDF copies of plots, use `--pdf`. To suppress viewing plots on the default display
device use `--noshow`. Combining these two options will produce only "hard" copies:
```
ncimonitor --pdf --noshow
```
