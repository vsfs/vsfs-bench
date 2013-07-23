"""Install and start/stop MongoDB.
"""

from __future__ import print_function, division
from fabric.api import task, env, execute, roles, run, local
import os
import socket
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball

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
    env.head_node_id = len(env.workers) + 2
    env.head_ip = socket.gethostbyname(env.head)

    env.roledefs = {}
    env.roledefs['head'] = [env.head]

load_config()


@roles('head')
def start_config_server():
    """Starts MongoDB config server.
    """
    run('%(mongo_bin)s --configsvr --dbpath %(config_dir)s' % env)


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
    pass
