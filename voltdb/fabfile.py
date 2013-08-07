"""Install and start/stop VoltDB
"""

from __future__ import print_function, division
from fabric.api import task, env, local
import os
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball

VERSION = '3.4'
URL = 'http://voltdb.com/downloads/technologies/server/' + \
      'LINUX-voltdb-%s.tar.gz' % VERSION
API_URL = 'http://voltdb.com/downloads/technologies/client/' + \
          'voltdb-client-cpp-linux-x86_64-3.0.tar.gz'

def load_config():
    env.voltdb_dir = 'voltdb-%s' % VERSION
    env.bin_dir = os.path.join(env.voltdb_dir, 'bin')
    env.voltdb = os.path.join(env.bin_dir, 'voltdb')

load_config()

@task
def download():
    """Downloads the VoltDB binaray and extract.
    """
    download_tarball(URL)
    download_tarball(API_URL)

@task
def build():
    """Builds the jar for the VoltDB.
    """
    local('%s compile -o vsfs.jar vsfs.sql' % env.voltdb)

@task
def start():
    pass


@task
def stop():
    pass
