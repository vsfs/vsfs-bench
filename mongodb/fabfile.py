"""Install and start/stop MongoDB.
"""

from __future__ import print_function, division
from fabric.api import task, env, execute, roles, run, local
import os
import socket
import pymongo
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball, run_background

URL = 'http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.4.5.tgz'


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


@roles('head')
def stop_config_server():
    run('pkill mongod')
    run('pkill mongos')


@task
def start():
    """Starts MongoDB cluster.
    """
    download_tarball(URL)

    if not os.path.exists(env.data_dir):
        os.makedirs(env.data_dir)

    execute(start_config_server)


@task
def stop():
    """Stops a MongoDB cluster.
    """
    execute(stop_config_server)
