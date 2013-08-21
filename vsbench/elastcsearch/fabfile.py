#!/usr/bin/env python
#
# Copyright 2013 (c) Ziling Huang <hzlgis@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Test against ElasticSearch
"""

from __future__ import print_function
from fabric.api import task, env
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))
from vsbench import fablib

URL = 'https://download.elasticsearch.org/elasticsearch/elasticsearch/' + \
      'elasticsearch-0.90.3.tar.gz'

fablib.load_config()

@task
def start():
    """Starts a ElasticSearch cluster.
    """
    print(env)
    pass


@task
def stop():
    """Stops a ElasticSearch cluster.
    """
    pass
