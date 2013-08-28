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

"""Install and start/stop VoltDB
"""

from __future__ import print_function, division
from fabric.api import task, env, execute, local, roles, run
from xml.dom import minidom
import os
import sys
sys.path.append('../..')
from vsbench import fablib

SCRIPT_DIR = os.path.dirname(__file__)
VERSION = '3.4'
URL = 'http://voltdb.com/downloads/technologies/server/' + \
      'LINUX-voltdb-%s.tar.gz' % VERSION
API_URL = 'http://voltdb.com/downloads/technologies/client/' + \
          'voltdb-client-cpp-linux-x86_64-3.0.tar.gz'


def load_config():
    fablib.load_nodes()
    env.voltdb_dir = os.path.join(SCRIPT_DIR, 'voltdb-%s' % VERSION)
    env.bin_dir = os.path.join(env.voltdb_dir, 'bin')
    env.voltdb = os.path.join(env.bin_dir, 'voltdb')

load_config()


@task
def download():
    """Downloads the VoltDB binaray and extract.
    """
    fablib.download_tarball(URL, output='voltdb-%s.tar.gz' % VERSION)
    fablib.download_tarball(API_URL)
    api_dir = fablib.base_dir(API_URL)
    local('rm -rf %s/include/boost' % api_dir)


@task
def build():
    """Builds the jar for the VoltDB.
    """
    local('%s compile -o vsfs.jar vsfs.sql' % env.voltdb)


def create_deployment_file(num_servers):
    """Creates deployment file for VoltDB.
    http://voltdb.com/docs/UsingVoltDB/ChapAppRun.php#RunClusterConfig
    """
    dom = minidom.parseString('<deployment/>')
    root = dom.firstChild
    cluster_node = minidom.Element('cluster')
    cluster_node.setAttribute('hostcount', str(num_servers))
    cluster_node.setAttribute('sitesperhost', str(1))
    cluster_node.setAttribute('kfactor', str(0))
    root.appendChild(cluster_node)

    paths_node = minidom.Element('paths')
    voltdbroot = minidom.Element('voltdbroot')
    voltdbroot.setAttribute('path', '/tmp')  # TODO(eddyxu) change to local
    paths_node.appendChild(voltdbroot)
    root.appendChild(paths_node)

    with open('deployment.xml', 'w') as fobj:
        fobj.write(dom.toprettyxml())


def start_others():
    fablib.run_background('%(voltdb)s create host %(head)s ' % env +
                          'catalog %s/vsfs.jar ' % SCRIPT_DIR +
                          'deployment %s/deployment.xml' % SCRIPT_DIR)

@roles('head')
@task
def start(num_servers):
    """Starts a VoltDB cluster.
    """
    num_servers = int(num_servers) - 1
    create_deployment_file(num_servers + 1)

    fablib.run_background('%(voltdb)s create host %(head)s ' % env +
                          'catalog %s/vsfs.jar ' % SCRIPT_DIR +
                          'deployment %s/deployment.xml' % SCRIPT_DIR)
    if num_servers:
        local('sleep 2')
        execute(start_others, hosts=env.workers[:num_servers])


@roles('head')
@task
def stop():
    """Stops the VoltDB cluster.
    """
    run('%(bin_dir)s/voltadmin shutdown --host=%(head)s ' % env)


@task
def ps():
    """Shows the runtime status of voltdb cluster.
    """
    fablib.ps('voltdb')
