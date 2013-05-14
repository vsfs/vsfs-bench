"""Install and start/stop VoltDB
"""

from __future__ import print_function, division
from fabric.api import task
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball

def load_config():
    pass


@task
def start():
    pass


@task
def stop():
    pass
