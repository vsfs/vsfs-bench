"""Install and start/stop VoltDB
"""

from __future__ import print_function, division
from fabric.api import task
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball

URL = 'http://voltdb.com/downloads/technologies/server/LINUX-voltdb-3.4.tar.gz'

def load_config():
    pass

@task
def download():
    download_tarball(URL)

@task
def start():
    pass


@task
def stop():
    pass
