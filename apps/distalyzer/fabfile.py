#/usr/bin/env python
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
import multiprocessing as mp
import numpy as np
import os
import shutil
import subprocess
import sys
sys.path.append('../..')
from vsbench import fablib

SCRIPT_DIR = os.path.dirname(__file__)
TRITON_SORT_GOOD_URL = 'http://www.macesystems.org/wp-uploads/2012/04/' \
                       'tritonsort_log_without_bad_node.tar.bz2'
TRITON_SORT_URL = 'http://www.macesystems.org/wp-uploads/2012/04/' \
                  'tritonsort_log_with_bad_node.tar.bz2'
HBASE_TRACE_URL = 'http://www.macesystems.org/wp-uploads/2012/03/' \
                  'wloadd_run11_hbase90-seeklog-Xmx2G-treemap.tar.bz2'

env['testdata'] = os.path.join(SCRIPT_DIR, 'testdata')
TESTDATA_DIR = os.path.join(SCRIPT_DIR, 'testdata')
TRITON_DIR = os.path.join(SCRIPT_DIR, fablib.base_dir(TRITON_SORT_URL))
# Distalyzer executable
DISTALYZER_EXEC = os.path.join(SCRIPT_DIR, 'distalyzer/Distalyzer.py')
HBASE_EXEC = os.path.join(SCRIPT_DIR,
                          'distalyzer/parsers/HBaseExecDistalyzer.py')
HBASE_DIR = os.path.join(SCRIPT_DIR, fablib.base_dir(HBASE_TRACE_URL))


@task
def download_traces():
    """Download traces
    """
    fablib.download_tarball(TRITON_SORT_URL)
    fablib.download_tarball(TRITON_SORT_GOOD_URL)


@task
def download():
    """Download Distalyzer source code.

    https://bitbucket.org/knagara/distalyzer
    """
    if os.path.exists(SCRIPT_DIR + '/distalyzer'):
        shutil.rmtree(SCRIPT_DIR + '/distalyzer')

    subprocess.check_call(
        'hg clone https://bitbucket.org/knagara/distalyzer',
        shell=True)

    with lcd('distalyzer'):
        local('python setup.py build')
        local('ln -s build/lib.linux-x86_64-2.7/ParseLogFiles.so')

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
    output_dir = os.path.join(TESTDATA_DIR, "output", os.path.basename(inpath))
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)
    cmd = '%s -i "%s/*" -j "%s/*" -S runtime -E End -o %s/origin-' % \
          (DISTALYZER_EXEC, inpath, inpath, output_dir)
    p = subprocess.Popen(cmd, shell=True)
    out, err = p.communicate()
    print(out)
    print(err)


def run_distalyzer_hbase(infile):
    output_dir = os.path.join(TESTDATA_DIR, "output", os.path.basename(infile))
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    cmd = '%s -i %s -o %s/hbase- -t 8' % (HBASE_EXEC, infile, output_dir)
    local(cmd)


def get_hbase_high_feature_counts():
    """Returns [(file, max, count), ...]
    """
    results = []
    split_dir = os.path.join(env['testdata'], 'hbase_split')
    for infile in sorted(os.listdir(split_dir)):
        fullpath = os.path.join(split_dir, infile)
        # print(fullpath)
        data = np.loadtxt(fullpath, skiprows=1, usecols=range(1,20))
        a = data[data[:,-2]>150]
        max_value = np.max(data[:,-2])
        count = len(a)
        results.append((infile, max_value, count))
    return results


@task
def prepare_hbase_traces():
    """
    """
    if os.path.exists(env['testdata']):
        shutil.rmtree(env['testdata'])
    split_dir = os.path.join(env['testdata'], 'hbase_split')
    os.makedirs(split_dir)
    with lcd(split_dir):
        local('split -l 10000 %s/requests-mod_*weight*' % HBASE_DIR)

    features = get_hbase_high_feature_counts()
    for f in features:
        print(f)

@task
def test_speedup():
    run_distalyzer_hbase('wloadd_run11_hbase90-seeklog-Xmx2G-treemap/' \
                         'requests-mod_processed_weight1.004631674')
