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
from fabric.api import task, env, execute, roles, run, local, settings
from fabric.api import parallel
import os
import pymongo
import shutil
import socket
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball, run_background

URL = 'http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.4.5.tgz'
CXX_DRIVER_URL = 'http://downloads.mongodb.org/cxx-driver/' \
                 'mongodb-linux-x86_64-2.4.5.tgz'


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
    run_background('%(mongo_bin)s --configsvr --dbpath %(config_dir)s' % env)
    run_background('%(bin_dir)s/mongos --configdb %(head)s' % env)


@parallel
def start_shared_server():
    run_background('%(mongo_bin)s --shardsvr --port 27017 '
                   '--dbpath %(data_dir)s' % env)


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
    download_tarball(CXX_DRIVER_URL, output='cxx_driver.tgz')

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
    print(env.workers)
    execute(start_shared_server, hosts=env.workers[:num_shard])

    local('sleep 2')
    conn = pymongo.Connection(env.head)
    admin = conn.admin
    for shard in env.workers[:num_shard]:
        admin.command('addshard', shard)


@task
def stop():
    """Stops a MongoDB cluster.
    """
    execute(stop_shared_server, hosts=env.workers)
    execute(stop_config_server)
