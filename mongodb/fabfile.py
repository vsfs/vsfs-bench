"""Install and start/stop MongoDB.
"""

from __future__ import print_function, division
from fabric.api import task
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball

URL = 'http://fastdl.mongodb.org/linux/mongodb-linux-x86_64-2.4.3.tgz'


def load_config():
    pass


@task
def start():
    download_tarball(URL)


@task
def stop():
    pass
