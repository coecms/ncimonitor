ncimonitor
==========

Extended usage information for the NCI system raijin.

.. image:: https://travis-ci.org/coecms/ncimonitor.svg?branch=master
   :target: https://travis-ci.org/coecms/ncimonitor
.. image:: https://circleci.com/gh/coecms/ncimonitor.svg?style=shield
  :target: https://circleci.com/gh/coecms/ncimonitor
.. https://codecov.io/github/coecms/ncimonitor/coverage.svg?branch=master
   :target: https://codecov.io/github/coecms/ncimonitor?branch=master
.. image:: https://landscape.io/github/coecms/ncimonitor/master/landscape.svg?style=flat
   :target: https://landscape.io/github/coecms/ncimonitor/master

To use:

.. code:: bash

    module purge
    module use /g/data3/hh5/public/modules
    module load conda/analysis27

Basic usage message:

.. code:: bash

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
      --pdf                 Save pdf copies of plots
      --noshow              Do not show plots
      -d, --delta           Show change since beginning of time period

Just invoking the program with no options will show a plot of your
default project Service Unit (SU) usage and short file usage.

You can ask for multiple projects, e.g.

.. code:: bash

    ncimonitor -P zz55 yy99 qq00

If you just want to see just the change in short file usage since the
start of the quarter (doesn't affect SU usage):

.. code:: bash

    ncimonitor --delta

Or usage by user:

.. code:: bash

    ncimonitor --byuser

Or just some selected users:

.. code:: bash

    ncimonitor -u usr1 -u usr2

You can change the max usage, which allows you to track individual users
against a target

.. code:: bash

    ncimonitor  -u usr1 -maxusage=1700

To save PDF copies of plots, use ``--pdf``. To suppress viewing plots on
the default display device use ``--noshow``. Combining these two options
will produce only hard copies:

::

    ncimonitor --pdf --noshow
