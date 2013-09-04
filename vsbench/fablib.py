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

"""Useful functions for fabfile
"""

from fabric.api import local, run, settings, env, parallel, execute, hide
from fabric.colors import yellow
import os
import time
from datetime import datetime

SCRIPT_DIR = os.path.dirname(__file__)
VSBENCH = os.path.abspath(os.path.join(SCRIPT_DIR, 'vsbench'))
VSFS_MOUNT = os.path.abspath(
    os.path.join(SCRIPT_DIR, '../lib/vsfs/vsfs/fuse/mount.vsfs'))


def base_dir(url):
    """Returns the base dir for a URL.
    """
    if url.endswith('.tar.bz') or url.endswith('tar.gz') \
            or url.endswith('.tar.xz'):
        url = url[:-7]
    elif url.endswith('.tgz'):
        url = url[:-4]
    return os.path.basename(url)


def result_filename(prefix, **kwargs):
    """Append the date to the prefix to be the result file name.

    @param prefix the prefix in the file name
    @return "prefix_2013_03_04_05_06.txt"
    """
    ext = kwargs.get('ext', '.txt')
    now = datetime.now()
    return prefix + '_' + now.strftime('%Y_%m_%d_%H_%M') + ext


def download_tarball(url, output=None):
    """Download the tarball file and uncompress it.
    """
    if not output:
        output = os.path.basename(url)
    basedir = base_dir(output)
    if os.path.exists(basedir):
        return False
    print('OUTPUT:', output)
    local("wget -O %s %s" % (output, url))
    filename = os.path.basename(output)
    if filename.endswith('.tar.gz') or filename.endswith('.tgz'):
        local("tar -xzf %s" % filename)
    elif filename.endswith('.tar.bz') or filename.endswith('.tar.bz2'):
        local("tar -xjf %s" % filename)
    return True


def run_background(cmd):
    """Run a remote task in background
    """
    run('nohup %s >& /dev/null < /dev/null &' % cmd, shell=True, pty=False)


def ps(regex, node_list=None):
    """Prints out the processes of the cluster.
    @param regex the regular express to grep out from ps(2)
    @node_list Optional. Provides a list of servers to query.
    """
    def _show_processes():
        run('ps aux | grep -E "%s" | grep -v grep || true' % regex)

    if not node_list:
        node_list = list(env.workers)
        node_list.append(env.head)
    with settings(hide('warnings', 'running', 'stderr', 'status')):
        execute(_show_processes, hosts=node_list)


def create_dir(path, mode=755):
    """Creates a new directory. If the directory already exists, then it
    deletes the directory first.
    """
    with settings(hide('warnings', 'running', 'stdout', 'stderr')):
        run('rm -rf %s' % path)
        run('mkdir -p %s' % path)
        run('chmod %d %s' % (mode, path))


def load_nodes():
    """Load nodes from /path/to/vsfs-bench/nodes.txt
    """
    nodes_file = os.path.join(os.path.dirname(__file__), '..', 'nodes.txt')
    try:
        with open(nodes_file) as fobj:
            node_list = [line.strip() for line in fobj]
    except IOError:
        raise UserWarning("a 'nodes.txt' file must be placed on vsfs-bench's "
                          "root directory.")
    env.head = node_list[0]
    env.nodes = node_list
    env.workers = node_list[1:]

    env.roledefs = {}
    env.roledefs['head'] = [env.head]
    env.roledefs['worker'] = [env.workers]


def create_indices(driver, num_indices, **kwargs):
    """Create indices
    """
    sleep = int(kwargs.get('sleep', 10))  # Sleep before creating indices.
    retry = int(kwargs.get('retry', 10))  # Retries on error.
    if sleep:
        local('sleep %d' % sleep)
    with settings(warn_only=True):
        cmd = '%s -driver %s -op create_indices -num_indices %d ' \
              '-%s_host=%s' % \
              (VSBENCH, driver, num_indices, driver, env.head)
        while retry:
            output = run(cmd)
            if not output.failed:
                break
            print "Retry creating indices on %s..%d" % (driver, retry)
            retry -= 1
            local('sleep 10')


@parallel
def insert_record_worker(driver, in_q, records, kwargs={}):
    """Parallel inserts records.

    @param driver driver name (e.g., hbase/mysql/vsfs)
    @param in_q a queue to put index ranges.
    @param records the number of records per index.

    Keyword Parameters:
    @param options Provide more options to the vsbench.
    """
    options = kwargs.get('options', '')
    idx_range = in_q.get()
    cmd = ('%s -driver %s -indices %s -records_per_index %d -op insert '
           '-%s_host=%s %s' %
           (VSBENCH, driver, idx_range, records, driver, env.head, options))
    run(cmd)


def search(driver, root, name, num_records, percent=0.1):
    """Search the namespace with [10%, 30%, 50%] records.
    @return the latency string of these queries.
    """
    def run_search(query):
        cmd = '%s -driver %s -%s_host %s -op search -query "%s"' % \
              (VSBENCH, driver, driver, env.head, query)
        output = run(cmd, combine_stderr=False)
        print output
    ret = ''
    start_key = int(num_records * 0.1)
    query = '%s/?%s>%d&%s<%d'
    start_time = time.time()
    end_key = int(start_key + percent * num_records)
    execute(run_search, query % (root, name, start_key, name, end_key),
            host=env.workers[0])
    end_time = time.time()
    ret = "%0.2f" % (end_time - start_time)
    return ret


def mount_vsfs(base_dir, mount_dir, host, options=None):
    """Use FUSE to mount VSFS
    @param base_dir the base directory to store the FUSE.
    @param mount_dir the mount point of vsfs-fuse.
    @param host the hostname of the primary master node.
    @param options the mount options
    """
    if not os.path.exists(base_dir):
        os.makedirs(base_dir)
    if not os.path.exists(mount_dir):
        os.makedirs(mount_dir)
    print(yellow("Trying to mount VSFS to %s." % mount_dir))
    run('%s -o nonempty -b %s -H %s -s object -o direct_io,big_writes %s' %
        (VSFS_MOUNT, base_dir, host, mount_dir))


def umount_vsfs(mount_dir):
    """Un-mount VSFS.
    """
    run('fusermount -u %s' % mount_dir)
