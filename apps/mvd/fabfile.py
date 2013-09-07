#!/usr/bin/env python
#
# Author: Lei Xu <eddyxu@gmail.com>

"""Run MVD tests
"""

from fabric.api import run, task, local, env, cd, parallel, execute, roles, lcd
import os
import time

NODE_FILE = os.path.join(os.path.dirname(__file__), '..', '..', 'nodes.txt')
SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
VSBENCH = os.path.join(SCRIPT_DIR, '../../bin/vsbench')


def load_config():
    """Init configurations.
    """
    try:
        with open(NODE_FILE) as fobj:
            node_list = [line.strip() for line in fobj]
    except IOError:
        raise UserWarning("Nodes.txt file does not exist.")
    env.nodes = node_list
    env.roledefs['nodes'] = env.nodes

load_config()


@task
def create_files(subfiles):
    """Prepare files for test
    """
    subfiles = int(subfiles)
    with cd(SCRIPT_DIR):
        local('./helper.py create -s %d abc' % subfiles)


@task
@parallel
@roles('nodes')
def umount_fuse():
    with cd(SCRIPT_DIR):
        run('fusermount -u mnt')


@parallel
def run_mvd(driver=None):
    job_id = env.all_hosts.index(env.host)
    tasks = len(env.all_hosts)
    duration = 0.1
    basedir = 'abc'
    print 'Job: %d' % job_id
    cmd = './helper.py mvd -d %f -j %d -t %d -b %s -f %s' % \
          (duration, job_id, tasks, basedir, 'file_list.txt')
    if driver:
        cmd += " -i index0 --driver %s --host %s" % (driver, env.nodes[0])

    with cd(SCRIPT_DIR):
        run(cmd)


@parallel
def perform_file_index(driver):
    index_cmd = '%s -op insert -driver %s -%s_host %s -num_indices 10 ' \
                '-records_per_index 10000' % \
                (VSBENCH, driver, driver, env.nodes[0])
    print(index_cmd)
    run(index_cmd)


@task
def run_index(driver):
    """Run indexing process. Param: (driver)
    """
    driver_dir = os.path.join(SCRIPT_DIR, '../../vsbench/%s' % driver)
    with lcd(driver_dir):
        local('fab stop')
        local('fab start:4')

    local('sleep 2')
    local('%s -op create_indices -driver %s -%s_host %s -num_indices 10 ' %
          (VSBENCH, driver, driver, env.nodes[0]))
    start = time.time()
    execute(perform_file_index, driver, hosts=env.nodes[4:20])
    print 'Execution Time: ', time.time() - start

    with lcd(driver_dir):
        local('fab stop')


@task
def run_with_index(driver):
    """
    """
    #local('fab -f ../../%s/fabfile.py stop' % driver)
    #local('fab -f ../../%s/fabfile.py start:4' % driver)
    local('../../bin/vsbench -driver %s -op create_indices -%s_host %s'
          '  -num_indices=1'
          % (driver, driver, env.nodes[0]))
    local('ls abc > file_list.txt')
    local('mkdir -p mnt/foo')
    start = time.time()
    execute(run_mvd, driver=driver, hosts=env.nodes[:10])
    print 'Execution Time: ', time.time() - start
    local('fab -f ../../%s/fabfile.py stop' % driver)


@task
def run_only_mvd(num_files=10000):
    """Run only mvd as base line.
    """
    local('ls abc | head -n %d > file_list.txt' % num_files)
    local('mkdir -p mnt/foo')
    start = time.time()
    execute(run_mvd, hosts=env.nodes[:10])
    print 'Execution Time: ', time.time() - start

    local('rm -rf mnt/foo')
