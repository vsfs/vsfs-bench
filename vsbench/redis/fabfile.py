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

""" Install/Manage Redis cluster.
"""

from __future__ import print_function, division
from fabric.api import task, env, execute, roles, run, local, settings, lcd
from fabric.api import parallel
import sys
sys.path.append('../..')
from vsbench.fablib import base_dir, download_tarball, run_background


URL = 'http://redis.googlecode.com/files/redis-2.8.2.tar.gz'

@task
def download():
    download_tarball(URL)
    with lcd(base_dir(URL)):
        local('make')


@task
def ps():
    """Report the running status of all servers.
    """
    pass
