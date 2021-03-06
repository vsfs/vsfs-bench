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
from fabric.api import task, local, cd, lcd, execute, run, roles, settings
from fabric.api import parallel, env
import csv
import gzip
import os
import subprocess
import multiprocessing as mp
import shutil
import sys
sys.path.append('../..')
from vsbench.hadoop import fabfile as hadoop
from vsbench.vsfs import fabfile as vsfs
from vsbench import fablib

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
VSFS_DIR = os.path.abspath(SCRIPT_DIR + "/../../vsbench/vsfs")
HADOOP_DIR = os.path.abspath(SCRIPT_DIR + "/../../vsbench/hadoop")
INPUT_DIR = os.path.join(SCRIPT_DIR, 'testdata/input')
VSFS_UTIL = os.path.join(SCRIPT_DIR, '../../lib/vsfs/vsfs/ui/cli/vsfs')
MOUNT_DIR = os.path.join(SCRIPT_DIR, 'testdata/mnt')
BASE_DIR = os.path.join(SCRIPT_DIR, 'testdata/base')
MRLOG = os.path.join(SCRIPT_DIR, 'mrlog.py')

TRITON_SORT_URL = 'http://www.macesystems.org/wp-uploads/2012/04/' \
                  'tritonsort_log_with_bad_node.tar.bz2'

__all__ = ['start', 'stop', 'index_inputs', 'download_traces', 'ps',
           'import_hive_data', 'parse_tritonsort_log', 'test_query_hive']


@task
def download_traces():
    """Download traces
    """
    fablib.download_tarball(TRITON_SORT_URL)


@task
@roles('head')
def start():
    """Starts a VSFS cluster and Hadoop cluster.
    """
    with lcd(VSFS_DIR):
        local('fab start:2,4')
    with lcd(HADOOP_DIR):
        local('fab start_hive:16')

    local('sleep 2')
    run('rm -rf %s' % BASE_DIR)
    run('rm -rf %s' % MOUNT_DIR)
    execute(fablib.mount_vsfs, BASE_DIR, MOUNT_DIR, vsfs.env['head'],
            host=vsfs.env['head'])


@task
@roles('head')
def stop():
    """Stops the VSFS cluster and the hadoop cluster.
    """
    with settings(warn_only=True):
        execute(fablib.umount_vsfs, MOUNT_DIR, host=vsfs.env['head'])
    with lcd(VSFS_DIR):
        local('fab stop')
    with lcd(HADOOP_DIR):
        local('fab stop')


@task
def ps():
    """List all related processes
    """
    with lcd(HADOOP_DIR):
        print('Check hadoop liveness')
        local('fab ps')
    with lcd(VSFS_DIR):
        print('Check vsfs liveness')
        local('fab ps')


def import_namespace():
    run('%s/mrlog.py --verbose import %s %s' %
        (SCRIPT_DIR, INPUT_DIR, MOUNT_DIR))
    run('%s/hadoop fs -copyFromLocal %s hdfs://%s/' %
        (hadoop.env['hadoop_bin'], INPUT_DIR, hadoop.env['head']))


@task
def index_inputs():
    """Parse all inputs and index them into vsfs's indices.

    Before running index_inputs(), the cluster must be first started.
    """
    execute(import_namespace, host=vsfs.env['head'])


def _parse_tritonsort_log(args):
    """
    """
    inpath, outpath, amplify_factor, seconds = args
    last_timestamp = None
    idx = 0
    with gzip.GzipFile(inpath) as logfile:
        outcsv = None
        csvwriter = None
        for line in logfile:
            fields = line.split()
            timestamp = float(fields[0])
            # One minute per file
            if not last_timestamp or last_timestamp < timestamp - seconds:
                last_timestamp = timestamp
                if outcsv:
                    outcsv.close()
                outcsv = open(outpath + "-%d.csv" % idx, 'w')
                csvwriter = csv.writer(outcsv, delimiter=',')
                idx += 1
            record_type = fields[1]  # event or state
            name = fields[2]
            value_name = ''
            value = ''
            if len(fields) > 3:
                value_name, value = fields[3].split('=')
            for i in range(amplify_factor):
                csvwriter.writerow([timestamp, record_type, name,
                                    value_name, value])
        if outcsv:
            outcsv.close()


@task
def parse_tritonsort_log(**kwargs):
    """Parses Tritonsort Log and generate CSV (param:amplify=30,nprocs=4,seconds=10)
    """
    amplify_factor = int(kwargs.get('amplify', 30))
    seconds = int(kwargs.get('seconds', 10))  # seconds per file.
    input_path = os.path.join(SCRIPT_DIR,
                              'tritonsort_log_with_bad_node/parsed')
    output_dir = os.path.join(SCRIPT_DIR, 'testdata/csv-x{}-{}s'.format(
        amplify_factor, seconds))
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    pool = mp.Pool()
    args = []
    for parsed_log in os.listdir(input_path):
        filename = parsed_log.split('.')[0]
        csvfile = os.path.join(output_dir, filename)
        args.append((os.path.join(input_path, parsed_log), csvfile,
                     amplify_factor, seconds))
    pool.map(_parse_tritonsort_log, args)


def _get_hadoop_csv_prefix(dirpath):
    filenames = os.listdir(dirpath)
    prefixes = { os.path.join(dirpath, fname.split('-')[0]) for fname in filenames }
    #print(prefixes)
    return list(prefixes)

@parallel
def _hadoop_copy_from_local(prefixes):
    """
    """
    idx = env.workers.index(env.host)
    base = 0
    while base + idx < len(prefixes):
        prefix = prefixes[base + idx]
        #print(prefix)
        cmd = "{}/hadoop fs -copyFromLocal {}-* hdfs://{}/csv/".format(
            hadoop.env['hadoop_bin'], prefix, hadoop.env['head']
        )
        run(cmd)
        base += len(env.workers)


@roles('head')
@task
def import_hive_data(csvdir='csv', **kwargs):
    """Import dataset into Hive as external table (param:csvdir='csv',create_index=1)

    Optional parameters:
    @param create_index=False
    """
    do_create_index = kwargs.get('create_index', 1)
    csv_dir = os.path.join(SCRIPT_DIR, 'testdata/%s' % csvdir)

    with settings(warn_only=True):
        result = run("%(hadoop_bin)s/hadoop fs -test -d hdfs://%(head)s/csv" %
                     hadoop.env)
        if result.return_code == 0:
            run("%(hadoop_bin)s/hadoop fs -rmr hdfs://%(head)s/csv" %
                hadoop.env)
        run("%(hadoop_bin)s/hadoop fs -mkdir hdfs://%(head)s/csv" %
            hadoop.env)

    #print(_get_hadoop_csv_prefix(csv_dir))
    csv_prefixes = _get_hadoop_csv_prefix(csv_dir)
    execute(_hadoop_copy_from_local, csv_prefixes, hosts=env.workers)

    # Initialize Hive SQL
    init_sql = os.path.join(SCRIPT_DIR, 'testdata/hive.sql')
    with open(init_sql, 'w') as sqlfile:
        sqlfile.write("""use default;
DROP TABLE IF EXISTS log;
CREATE EXTERNAL TABLE log (time double, type string, event string,
value_name string, value double )
COMMENT "log table"
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://%(head)s/csv';

DROP TABLE IF EXISTS log_noidx;
CREATE EXTERNAL TABLE log_noidx (time double, type string, event string,
value_name string, value double )
COMMENT "log table without index"
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://%(head)s/csv';

SELECT * FROM log LIMIT 3;
SELECT * FROM log_noidx LIMIT 3;
""" % hadoop.env)
        if do_create_index:
            sqlfile.write("""
CREATE INDEX idx ON TABLE log(value_name)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;
ALTER INDEX idx ON log REBUILD;
CREATE INDEX idx_value ON TABLE log(value)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD;
ALTER INDEX idx_value ON log REBUILD;
set hive.optimize.autoindex=true;
SHOW INDEX ON log;
""")

    with cd(SCRIPT_DIR):
        hadoop.run_hive("-f %s" % init_sql)


HIVE_SQL_TEMPLATE = """SELECT hour, count(hour) AS hrcount FROM
(SELECT round(time / 60) AS hour FROM %s WHERE
value > %d) t2 GROUP BY hour ORDER BY hrcount DESC LIMIT 3;
"""

@roles('head')
def _test_query_origin_hive(threshold):
    print("Running on Log")
    with cd(SCRIPT_DIR):
        hadoop.run_hive('-e "%s"' % (HIVE_SQL_TEMPLATE % ('log', threshold)))
    print("Running on Log without index.")
    with cd(SCRIPT_DIR):
        hadoop.run_hive('-e "%s"' % (HIVE_SQL_TEMPLATE % ('log_noidx', threshold)))

@parallel
def _extract_features(testdir, threshold):
    with cd(SCRIPT_DIR):
        prefixes = _get_hadoop_csv_prefix(testdir)
        idx = env.workers.index(env.host)
        base = 0
        while base + idx < len(prefixes):
            prefix = prefixes[base + idx]
            cmd = "{} extract -t {} -p {} -o {}/features/feature-{}.txt " \
                  "{}".format(
                      MRLOG, threshold, prefix, SCRIPT_DIR, env.host,
                      testdir)
            run(cmd)

            base += len(env.workers)

def prepare_hive_query_data(threshold=100000):
    pass

@task
@roles('head')
def test_query_hive(threshold=1000000, csv_dir='csv'):
    """Run query results on Hive (param:threshold=1000000)
    """
    threshold = int(threshold)

    #execute(_test_query_origin_hive, threshold)

    data_dir = '{}/testdata/{}'.format(SCRIPT_DIR, csv_dir)
    # Hive on VSFS
    with settings(warn_only=True):
        result = run("%(hadoop_bin)s/hadoop fs -test -d "
                     "hdfs://%(head)s/hivevsfs" % hadoop.env)
        if result.return_code == 0:
            run("%(hadoop_bin)s/hadoop fs -rmr hdfs://%(head)s/hivevsfs" %
                hadoop.env)
    run("%(hadoop_bin)s/hadoop fs -mkdir hdfs://%(head)s/hivevsfs" %
        hadoop.env)

    with cd(SCRIPT_DIR):
        run('rm -rf features && mkdir -p features')
    with settings(warn_only=True):
        execute(_extract_features, data_dir, threshold, hosts=env.workers)

    desired_files = []
    for filename in os.listdir('features'):
        file_path = os.path.join(SCRIPT_DIR, 'features', filename)
        with open(file_path) as fobj:
            for line in fobj:
                fields = line.split()
                print(fields)
                if fields[1] == 'Writer_5_runtime' and \
                    float(fields[2]) > threshold:
                    desired_files.append(fields[0])

    print("Desired files: ", desired_files)
    for filename in desired_files:
        run("%s/hadoop fs -copyFromLocal %s/%s hdfs://%s/hivevsfs" %
            (hadoop.env['hadoop_bin'], data_dir, filename, hadoop.env['head']))

    sql_create_vsfs_table = """
DROP TABLE IF EXISTS vsfs;
CREATE EXTERNAL TABLE vsfs (time double, type string, event string,
value_name string, value double)
COMMENT 'Hive On Vsfs'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'hdfs://%(head)s/hivevsfs';
""" % hadoop.env
    hadoop.run_hive('-e "%s"' % sql_create_vsfs_table)

    with cd(SCRIPT_DIR):
        hadoop.run_hive('-e "%s"' % (HIVE_SQL_TEMPLATE % ('vsfs', threshold)))
