#!/usr/bin/env python
#
# Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Install and start/stop MongoDB.
"""

from __future__ import print_function, division
import fabric
from fabric.api import task, env, execute, roles, run, local, settings, lcd
from fabric.api import parallel
from fabric.colors import yellow, blue, green, red
from multiprocessing import Queue
import os
import pymongo
import shutil
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball, run_background

SCRIPT_DIR = os.path.dirname(__file__)
MONGO_VERSION = '2.5.1'
URL = 'http://fastdl.mongodb.org/linux/' \
      'mongodb-linux-x86_64-%s.tgz' % MONGO_VERSION
CXX_DRIVER_URL = 'http://downloads.mongodb.org/cxx-driver/' \
                 'mongodb-linux-x86_64-%s.tgz' % MONGO_VERSION
VSBENCH = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, 'bin/vsbench'))
MONGOS_PORT = 27018


def load_config():
    """Load configurations and initialize environment.
    """
    env.mongo_dir = base_dir(URL)
    env.bin_dir = os.path.join(env.mongo_dir, 'bin')
    env.config_dir = os.path.abspath('testdata')
    env.data_dir = os.path.abspath('testdata/data')
    env.mongo_bin = os.path.join(env.bin_dir, 'mongod')

    with open('../nodes.txt') as fobj:
        node_list = [line.strip() for line in fobj]
    env.head = node_list[0]
    env.workers = node_list[1:]
    env.roledefs = {}
    env.roledefs['head'] = [env.head]

load_config()


@roles('head')
def start_config_server():
    """Starts MongoDB config server.
    """
    run_background('%(mongo_bin)s --configsvr --port 27019 ' \
                   '--dbpath %(config_dir)s' % env)
    run('sleep 2')
    #run('%(bin_dir)s/mongos --port --configdb %(head)s' % env)
    run_background('%(bin_dir)s/mongos --port 27018 --configdb %(head)s' % env)


@parallel
def start_shared_server():
    run('mkdir -p %(data_dir)s/%(host)s' % env)
    run_background('%(mongo_bin)s --shardsvr --port 27017 '
                   '--dbpath %(data_dir)s/%(host)s' % env)


@roles('head')
def stop_config_server():
    """Stops the configsrv and mongs
    """
    with settings(warn_only=True):
        run('pkill mongod')
        run('pkill mongos')


@parallel
def stop_shared_server():
    """Stops all shared servers.
    """
    with settings(warn_only=True):
        run('pkill mongod')


@task
def download():
    download_tarball(URL)
    download_tarball(CXX_DRIVER_URL, output='mongo-cxx-driver-v2.5.tgz')
    with lcd('mongo-cxx-driver-v2.5'):
        local('scons')


@roles('head')
@task
def start(num_shard):
    """Starts MongoDB cluster.

    @param num_shard the number of shard servers.
    """
    num_shard = int(num_shard)
    download_tarball(URL)

    if os.path.exists(env.config_dir):
        shutil.rmtree(env.config_dir)

    os.makedirs(env.data_dir)

    execute(start_config_server)
    execute(start_shared_server, hosts=env.workers[:num_shard])

    local('sleep 5')
    print(yellow('Initialize MongoDB'))
    conn = pymongo.Connection('%s:27018' % env.head)
    print(yellow('MongoDB connected'))
    admin = conn.admin
    for shard in env.workers[:num_shard]:
        admin.command('addshard', '%s:27017' % shard)
    admin.command('enableSharding', 'vsfs')


@roles('head')
@task
def stop():
    """Stops a MongoDB cluster.
    """
    print(yellow('Stopping shared servers..'))
    execute(stop_shared_server, hosts=env.workers)
    print(yellow('Stopping config server..'))
    execute(stop_config_server)


def _show_processes():
    """ Show mysql process on remote machine.
    """
    run('ps aux | grep mongo | grep -v grep || true')


@task
def status():
    """Query the running status of the test cluster.
    """
    node_list = list(env.workers)
    node_list.append(env.head)
    fabric.state.output['running'] = False
    fabric.state.output['stderr'] = False
    execute(_show_processes, hosts=node_list)


@roles('head')
def import_files(num_files):
    """Build a namespace in MongoDB first.
    """
    cmd = '%s -driver mongodb -mongodb_host %s -mongodb_port %d -op import ' \
          '-records_per_index %d' % \
          (VSBENCH, env.head, MONGOS_PORT, num_files)
    print(green('Importing files: running %s' % cmd))
    run(cmd)


def insert_records(shard, indices, records):
    """
    """
    print(yellow('Insert records on %d shard, %d indices, %d records.' %
                 (shard, indices, records)))
    execute(stop)
    local('sleep 5')
    in_q = Queue()
    execute(start, shard)
    local('sleep 5')
    execute(import_files, records)

    execute(stop)


@task
def test_insert(shard, indices, records):
    """Test inserting performance (param: shard, indices, records)
    """
    insert_records(int(shard), int(indices), int(records))
