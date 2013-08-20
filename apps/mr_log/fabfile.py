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
import numpy as np
import os
import random
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
    @param zipf
    @param error error possiblity
    @param days
    """
    zipf_a = 1.5
    error_poss = 0.05   # 5% of error possiblity
    events_per_second = 10

    COMPONENTS = ['WEB', 'SQL', 'MQ', 'NET', 'BALANCER']
    zipf_dist = {}
    for comp in COMPONENTS:
        zipf_dist[comp] = np.random.zipf(zipf_a, 365 * 24)
        random.shuffle(zipf_dist[comp])

    if os.path.exists(INPUT_DIR):
        shutil.rmtree(INPUT_DIR)
    os.makedirs(INPUT_DIR)

    logname_format = 'log_%d.%d.%d.%d.txt'  # log_YEAR.MONTH.DAY.HOUR.txt

    date = datetime.date(2013, 1, 1)
    delta = datetime.timedelta(1)  # one day
    hours = 0
    while date.year == 2013:
        date += delta
        for hr in range(24):
            logfile = logname_format % (date.year, date.month, date.day, hr)
            comps_possibilities = \
                map(float, [zipf_dist[c][hours] for c in COMPONENTS])
            error_poss_array = np.cumsum(comps_possibilities)
            error_poss_array /= float(error_poss_array[-1])
            hours += 1
            with open(os.path.join(INPUT_DIR, logfile), 'w') as fobj:
                for i in range(3600 * events_per_second):
                    poss = random.random()
                    if poss <= error_poss:
                        poss /= error_poss
                        idx = 0
                        comp = None
                        while True:
                            if poss <= error_poss_array[idx]:
                                comp = COMPONENTS[idx]
                                break
                            idx += 1

                        error_line = \
                            '[ERROR][%s] Has an error to figure out...\n' % \
                            comp
                        fobj.write(error_line)
                    else:
                        fobj.write('[OK] Another successful event.\n')


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
