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


from fabric.api import task, local, lcd, execute, run
import datetime
import numpy as np
import os
import random
import shutil
import sys
sys.path.append('../..')
from vsbench.hbase import fabfile as hbase
from vsbench.vsfs import fabfile as vsfs
from vsbench import fablib

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
VSFS_DIR = os.path.abspath(SCRIPT_DIR + "/../../vsbench/vsfs")
HBASE_DIR = os.path.abspath(SCRIPT_DIR + "/../../vsbench/hbase")
INPUT_DIR = os.path.join(SCRIPT_DIR, 'testdata/input')
VSFS_UTIL = os.path.join(SCRIPT_DIR, '../../lib/vsfs/vsfs/client/vsfs')
MOUNT_DIR = os.path.join(SCRIPT_DIR, 'testdata/mnt')
BASE_DIR = os.path.join(SCRIPT_DIR, 'testdata/base')

__all__ = ['start', 'stop', 'gen_input', 'index_inputs']


@task
def gen_input(**kwargs):
    """Generate input dataset statistically.

    Optional parameters:
    @param zipf The alpha value for zipf distribution.
    @param error error possibility
    @param events_per_second How many events recorded for one second.
    @param days
    """
    zipf_a = float(kwargs.get('zipf', 1.5))
    # Error possibility
    error_poss = float(kwargs.get('errors', 0.05))   # 5% of error possiblity
#    days = int(kwargs.get('days', 365))
    events_per_second = int(kwargs.get('events_per_second', 10))

    # Assume we have 5 individual components which fail indipendently.
    COMPONENTS = ['WEB', 'MYSQL', 'QUEUE', 'NETWORK', 'BALANCER']
    zipf_dist = {}
    for comp in COMPONENTS:
        zipf_dist[comp] = np.random.zipf(zipf_a, 365 * 24)
        random.shuffle(zipf_dist[comp])

    if os.path.exists(INPUT_DIR):
        shutil.rmtree(INPUT_DIR)
    os.makedirs(INPUT_DIR)

    logname_format = 'log_%d.%02d.%02d.%02d.txt'  # log_YEAR.MONTH.DAY.HOUR.txt

    date = datetime.date(2013, 1, 1)
    delta = datetime.timedelta(1)  # one day
    hours = 0
    while date.year == 2013 and date.month == 1:
        for hr in range(24):
            logfile = logname_format % (date.year, date.month, date.day, hr)
            comps_possibilities = \
                map(float, [zipf_dist[c][hours] for c in COMPONENTS])
            error_poss_array = np.cumsum(comps_possibilities)
            error_poss_array /= float(error_poss_array[-1])
            hours += 1
            # Generates log for one hour.
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
        date += delta


@task
def start():
    """Starts a VSFS cluster and Hadoop cluster.
    """
    with lcd(VSFS_DIR):
        local('fab start:4')
    with lcd(HBASE_DIR):
        local('fab start:16')

    local('sleep 2')
    run('rm -rf %s' % BASE_DIR)
    run('rm -rf %s' % MOUNT_DIR)
    execute(fablib.mount_vsfs, BASE_DIR, MOUNT_DIR, vsfs.env['head'],
            host=vsfs.env['head'])


@task
def stop():
    """Stops the VSFS cluster and the hadoop cluster.
    """
    execute(fablib.umount_vsfs, MOUNT_DIR, host=vsfs.env['head'])
    with lcd(VSFS_DIR):
        local('fab stop')
    with lcd(HBASE_DIR):
        local('fab stop')



def import_namespace():
    run('%s/mrlog.py --verbose import %s %s' % \
        (SCRIPT_DIR, INPUT_DIR, MOUNT_DIR))
    run('%s/hadoop fs -copyFromLocal %s hdfs://%s/' %
        (hbase.env['hadoop_bin'], INPUT_DIR, hbase.env['head']))


@task
def index_inputs():
    """Parse all inputs and index them into vsfs's indices.

    Before running index_inputs(), the cluster must be first started.
    """
    execute(import_namespace, host=vsfs.env['head'])
