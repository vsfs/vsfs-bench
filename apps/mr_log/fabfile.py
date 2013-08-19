#!/usr/bin/env python
#
# Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from fabric.api import task, local, lcd, execute
import datetime
import os
import shutil
import sys
sys.path.append('../..')
from vsbench.hbase import fabfile as hbase

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
VSFS_DIR = os.path.abspath(SCRIPT_DIR + "/../../vsbench/vsfs")
INPUT_DIR = os.path.join(SCRIPT_DIR, 'testdata/input')

__all__ = ['start', 'stop', 'gen_input']


@task
def gen_input():
    """Generate input dataset statistically.
    """
    if os.path.exists(INPUT_DIR):
        shutil.rmtree(INPUT_DIR)
    os.makedirs(INPUT_DIR)

    logname_format = 'log_%d.%d.%d.%d.txt'  # log_YEAR.MONTH.DAY.HOUR.txt

    date = datetime.date(2013, 1, 1)
    delta = datetime.timedelta(1)  # one day
    while date.year == 2013:
        date += delta
        for hr in range(24):
            logfile = logname_format % (date.year, date.month, date.day, hr)
            print(logfile)
            with open(os.path.join(INPUT_DIR, logfile), 'w') as fobj:
                pass


@task
def start():
    """Starts a VSFS cluster and Hadoop cluster.
    """
    with lcd(VSFS_DIR):
        local('fab start:4')
    execute(hbase.start, 16, hbase=False)


@task
def stop():
    """Stops the VSFS cluster and the hadoop cluster.
    """
    with lcd(VSFS_DIR):
        local('fab stop')
    execute(hbase.stop, hbase=False)
