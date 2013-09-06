#!/usr/bin/env python
#
# Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
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

from __future__ import print_function
from fabric.api import env, task, local, lcd
import gzip
import os
import shutil
import subprocess
import multiprocessing as mp
import sys
sys.path.append('../..')
from vsbench import fablib

SCRIPT_DIR = os.path.dirname(__file__)
TRITON_SORT_URL = 'http://www.macesystems.org/wp-uploads/2012/04/' \
                  'tritonsort_log_with_bad_node.tar.bz2'

env['testdata'] = os.path.join(SCRIPT_DIR, 'testdata')
TESTDATA_DIR = os.path.join(SCRIPT_DIR, 'testdata')
TRITON_DIR = os.path.join(SCRIPT_DIR, fablib.base_dir(TRITON_SORT_URL))
# Distalyzer executable
DISTALYZER_EXEC = os.path.join(SCRIPT_DIR, 'distalyzer/Distalyzer.py')

@task
def download_traces():
    """Download traces
    """
    fablib.download_tarball(TRITON_SORT_URL)


@task
def download():
    """Download Distalyzer source code.

    https://bitbucket.org/knagara/distalyzer
    """
    if os.path.exists(SCRIPT_DIR + '/distalyzer'):
        shutil.rmtree(SCRIPT_DIR + '/distalyzer')

    subprocess.check_call(
        'hg clone -r v0.1 https://bitbucket.org/knagara/distalyzer',
        shell=True)


def parse_tritonsort_logfile(args):
    """Parses the tritonsort log file and split it.
    """
    print(args)
    inpath, outpath, duration = args
    last_timestamp = None
    idx = 0
    with gzip.GzipFile(inpath) as logfile:
        outfile = None
        for line in logfile:
            fields = line.split()
            timestamp = float(fields[0])
            if not last_timestamp or last_timestamp < timestamp - duration:
                last_timestamp = timestamp
                if outfile:
                    outfile.close()
                outfile = gzip.GzipFile(outpath + '-%d.gz' % idx, 'w')
                idx += 1
            outfile.write(line + '\n')
        if outfile:
            outfile.close()

@task
def prepare_traces():
    """Prepare traces
    """
    if os.path.exists(env['testdata']):
        shutil.rmtree(env['testdata'])
    os.makedirs(env['testdata'])
    origin_path = os.path.join(TESTDATA_DIR, 'origin')
    local('cp -r %s/parsed %s' % (TRITON_DIR, origin_path))

    pool = mp.Pool(8)
    for duration in [1, 10, 30]:
        output_dir = os.path.join(TESTDATA_DIR, 'duration_%d' % duration)
        os.makedirs(output_dir)
        args = []
        for parsed_log in os.listdir(origin_path):
            filename = parsed_log.split('.')[0]
            infile = os.path.join(origin_path, parsed_log)
            outfile = os.path.join(output_dir, filename)
            pool.apply_async(parse_tritonsort_logfile,
                             (infile, outfile, duration))
            args.append((infile, outfile, duration))
        pool.map(parse_tritonsort_logfile, args)


def run_distalyzer(inpath):
    """
    """
    output_dir = os.path.join(TESTDATA_DIR, "output", os.path.dirname(inpath))
    cmd = '%s -i "%s/*" -j "%s/*" -S runtime -E End -o %s/out-' % \
          (DISTALYZER_EXEC, inpath, inpath, output_dir)
    p = subprocess.Popen(cmd, shell=True)
    out, err = p.communicate()
    print(out)
    print(err)


@task
def test_speedup():
    run_distalyzer(os.path.join(TESTDATA_DIR, 'origin'))
