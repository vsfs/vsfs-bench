#!/usr/bin/env python
#
# Fabric configurations for VSFS
#
# Copyright 2013 (c) Ziling Huang <hzlgis@gmail.com>

"""Vsfs fabfile
"""

from fabric.api import run, roles, task, env, execute, parallel, local
from fabric.api import settings, cd, hide, sudo
from fabric.colors import green as _green, yellow as _yellow
from multiprocessing import Queue
import boto
import boto.ec2
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import fablib
import time
import pwd


SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
NODE_FILE = os.path.join(SCRIPT_DIR, '../../nodes.txt')
MASTERD = os.path.abspath(
    os.path.join(SCRIPT_DIR, '../../lib/vsfs/vsfs/masterd/masterd'))
INDEXD = os.path.abspath(
    os.path.join(SCRIPT_DIR, '../../lib/vsfs/vsfs/indexd/indexd'))
LOG_DIR = os.path.join(SCRIPT_DIR, 'log')
VSFSUTIL = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir,
                                        'client', 'vsfs'))
USER = pwd.getpwuid(os.getuid())[0]
MASTERD_DIR = os.path.join('/scratch', USER, 'vsfs/masterd')
INDEXD_DIR = os.path.join('/scratch', USER, 'vsfs/indexd')

BASE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, 'base_dir'))
MNT_POINT = os.path.abspath(os.path.join(SCRIPT_DIR, 'mnt'))
FUSE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir,
                           'lib/vsfs/vsfs/fuse'))
ITERATIONS = 3

FILEBENCH_WORKLOADS = ['fileserver', 'oltp', 'webserver']
TEST_DIR = MNT_POINT
NUM_FILES = '100000'
NUM_THREADS = '16'
MEAN_FILE_SIZE = '4k'
IO_SIZE = '1024'
RUN_TIME = '60'

FILEBENCH_CONF_FILE = os.path.abspath(os.path.join(SCRIPT_DIR,
                                      'filebench.conf'))


def load_config():
    """Init configurations.
    """
    try:
        with open(NODE_FILE) as fobj:
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

    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

load_config()

@parallel(pool_size=10)
def prepare_directories():
    """Make dir for Master, Index Server, Meta Server.
    """
    fablib.create_dir(MASTERD_DIR)
    fablib.create_dir(INDEXD_DIR)


@parallel
def start_master():
    """Starts the master daemon.
    """
    run('%s -primary -daemon -dir %s -log_dir %s' %
        (MASTERD, MASTERD_DIR, LOG_DIR))


@roles('head')
def stop_master():
    """Stops the master node.
    """
    run('pkill masterd || true')


def start_index_server(options='', **kwargs):
    """Starts an IndexServer.

    Keyword argument:
    @param master the address of master node.
    """
    master_node = kwargs.get('master', env.head)
    cmd = '%s -master_addr %s -daemon -log_dir %s ' \
          '-datadir %s ' \
          '-update_immediately %s' % \
          (INDEXD, master_node, LOG_DIR, INDEXD_DIR, options)
    if kwargs.get('profile', False):
        cmd = 'CPUPROFILE=indexd.prof ' + cmd
    with cd(SCRIPT_DIR):
        run(cmd)


@parallel(pool_size=10)
def stop_index_server():
    run('pkill indexd || true')


@task
@roles('head')
def start(nodes, **kwargs):
    """Starts a VSFS cluster

    @param nodes num of index nodes in the cluster.

    Keyword parameters:
    @param clusters The default value is 1.
    """
    num_nodes = int(nodes)
    num_clusters = int(kwargs.get('clusters', 1))  # How many master node.

    total_nodes = num_nodes + num_clusters
    if total_nodes > len(env.nodes):
        raise RuntimeError("Total nodes is too much: %d" % total_nodes)
    master_nodes = env.nodes[:num_clusters]
    execute(prepare_directories, hosts=env.nodes)

    for master in master_nodes:
        execute(start_master, host=master)
    run('sleep 5')

    num_workers_per_cluster = num_nodes / num_clusters
    for i in range(num_clusters):
        start_node = num_clusters + i * num_workers_per_cluster
        end_node = min(num_clusters + (i + 1) * num_workers_per_cluster,
                       len(env.nodes))
        master = master_nodes[i]
        index_servers = env.workers[start_node:end_node]
        execute(start_index_server, master=master, hosts=index_servers)


@task
def stop():
    """Stops the VSFS cluster.
    """
    with settings(
            hide('warnings', 'running', 'stdout', 'stderr')):
        execute(stop_index_server, hosts=env.workers)
        execute(stop_master, hosts=env.nodes)


def insert_records(num_indices, client_nodes, records):
    """
    @param num_indices the number of indices to insert.
    @param client_nodes the addresses of each client node.
    @param records the records to inserted into each index.

    @return the latency of inserting records.
    """
    idx_per_client = num_indices / len(client_nodes)
    in_q = Queue()
    for i in range(len(client_nodes)):
        in_q.put('%d-%d' % (1 + i * idx_per_client,
                 min(num_indices, (i + 1) * idx_per_client)))
    start_time = time.time()
    with settings(warn_only=True):
        execute(fablib.insert_record_worker, 'vsfs', in_q, records,
                {'options': '-vsfs_client_num_thread 16' +
                 ' -vsfs_client_enable_cache -batch_size 4096'},
                hosts=client_nodes)
    end_time = time.time()
    return end_time - start_time


def test_insert(num_nodes, indices, records):
    """Insert records into vsfs.
    """
    local('sleep 5')
    execute(start, num_nodes)
    execute(fablib.create_indices, 'vsfs', indices, host=env.head)

    #client_nodes = env.workers[-5:]
    client_nodes = env.workers
    insert_latency = insert_records(indices, client_nodes, records)

    search_latencies = fablib.search('vsfs', '/foo/bar', 'index.0', records)
    execute(stop)

    return '%d %d %0.2f %s' % (records * indices, num_nodes,
                               insert_latency, search_latencies)


@task
def test_scale(**kwargs):
    """VSFS Scalability test.
    """
    repeat = int(kwargs.get('repeat', 3))
    indices = int(kwargs.get('indices', 500))
    records = int(kwargs.get('records', 20000))

    execute(stop)
    result_file = fablib.result_filename('test_scale')
    with open(result_file, 'w') as result_file:
        result_file.write("""# Result for test_scale.
# Number of indices: %d
# Number of records per index: %d
# Records indexd metad time 10%% 30%% 50%% iteration
""" % (indices, records))
        for nodes in range(2, 17, 2):
            for iteration in range(repeat):
                output = test_insert(nodes, indices, records)
                result_file.write("%s %d\n" % (output, iteration))
                result_file.flush()
                os.fsync(result_file.fileno())


@task
def test_data_migration():
    print "Starting vsfs data migration test..."


def config_filebench(workload, num_files, num_threads, test_dir):
    print(workload, test_dir, num_files, num_threads, MEAN_FILE_SIZE, IO_SIZE, RUN_TIME)
    filebench_conf = """load %s
set $dir=%s
set $nfiles=%d
set $nthreads=%d
set $meanfilesize=%s
set $iosize=%s
run %s
""" % (workload, test_dir, num_files, num_threads, MEAN_FILE_SIZE,
       IO_SIZE, RUN_TIME)
    print(filebench_conf)
    with open(FILEBENCH_CONF_FILE, 'w') as f:
        f.write(filebench_conf)


def run_filebench():
    if os.path.exists(os.path.join(TEST_DIR, 'bigfileset')):
        run('rm -rf %s' % os.path.join(TEST_DIR, 'bigfileset'))
    throughput = run("filebench -f %s | awk ' /Summary/ {print $7}'"
                     % (FILEBENCH_CONF_FILE))
    return throughput


def mount_vsfs():
    """Mount VSFS on lustre.
    """
    vsfs_path = os.path.join(FUSE_DIR, 'mount.vsfs')
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
    if not os.path.exists(MNT_POINT):
        os.makedirs(MNT_POINT)
    print(_yellow("Trying to mount VSFS. "))
    run('%s -o nonempty -b %s -H %s %s' %
        (vsfs_path, BASE_DIR, env.head, MNT_POINT))


def umount_vsfs():
    """Un-mount VSFS.
    """
    run('fusermount -u %s', MNT_POINT)


@task
@roles('head')
def test_filebench_with_vsfs(**kwargs):
    """Run Filebench on lustre with vsfs mounted.
       Keyword options:
       @param num_files
       @param num_threads
       @param test_dir
    """
    num_files = int(kwargs.get('num_files', '100000'))
    num_threads = int(kwargs.get('num_threads', '16'))
    test_dir = kwargs.get('test_dir', MNT_POINT)
    mount_vsfs()
    with open('test_filebench_with_vsfs', 'w') as result_file:
        result_file.write('Workload #Threads Throughput iteration\n')
    for workload in FILEBENCH_WORKLOADS:
        config_filebench(workload, num_files, num_threads, test_dir)
        print "Running Filebench workload: %s with VSFS." % workload
        for i in range(ITERATIONS):
            throughput = run_filebench()
            with open('test_filebench_with_vsfs', 'a') as result_file:
                result_file.write("%s  %s  %s  %s\n"
                                  % (workload, num_threads, throughput, i))


@task
@roles('head')
def test_filebench_without_vsfs(**kwargs):
    """Run Filebench on lustre without vsfs mounted.
       Keyword options:
       @param num_files
       @param num_threads
       @param test_dir
    """
    load_config()
    num_files = kwargs.get('num_files', '100000')
    num_threads = kwargs.get('num_threads', '16')
    test_dir = kwargs.get('test_dir', MNT_POINT)
    with open('test_filebench_without_vsfs', 'w') as result_file:
        result_file.write('Workload #Threads Throughput iteration\n')
    for workload in FILEBENCH_WORKLOADS:
        config_filebench(workload, num_files, num_threads, test_dir)
        print "Running Filebench workload: %s without VSFS." % workload
        for i in range(ITERATIONS):
            throughput = run_filebench()
            with open('test_filebench_without_vsfs', 'a') as result_file:
                result_file.write("%s  %s  %s  %s\n"
                                  % (workload, num_threads, throughput, i))


@task
@roles('head')
def test_filebench_all(**kwargs):
    """Run both tests.
       Keyword options:
       @param num_files
       @param num_threads
       @param test_dir
    """
    test_filebench_without_vsfs(kwargs)
    test_filebench_with_vsfs(kwargs)


@task
@roles('head')
def stress_index_server():
    """Use all nodes to stress one index server.
    """
    execute(stop)
    run('sleep 5')

    execute(prepare_directories, hosts=[env.head])
    execute(prepare_directories, hosts=env.workers[:2])
    start_master()
    run('sleep 5')
    execute(start_index_server, hosts=env.workers[:2])
    run('sleep 5')

    indices = 200
    records = 100000
    execute(fablib.create_indices, 'vsfs', indices, host=env.head)
    insert_latency = insert_records(indices, env.workers, records)
    print insert_latency
    run('%s info -H %s /foo/bar' % (VSFSUTIL, env.head))

    execute(stop)


EC2_REGION = 'us-east-1'
EC2_AMI = 'ami-90374bf9'  # Ubuntu server 13.04 instance (w/o EBS)
EC2_SECURITY_GROUPS = ['quick-start-1']


@task
def ec2_create_instance():
    """Creates an EC2 instance for VSFS
    """
    print(_yellow('Creating EC2 instance'))
    conn = boto.ec2.connect_to_region(EC2_REGION)
    conn.run_instances(EC2_AMI,
                       instance_type='m1.small',
                       security_groups=EC2_SECURITY_GROUPS,
                       key_name='eddy')

    reservation = conn.get_all_instances()
    instance = reservation[0].instances[0]
    instance.add_tag("Name", "VSFS AMI")


@task
def ec2_terminate_instance():
    """Terminates the running EC2 instance.
    """
    pass


def ec2_install_packages():
    packages = ['g++', 'autoconf-archive', 'libleveldb-dev', 'libsnappy-dev',
                'libevent-dev', 'libfuse-dev', 'libattr1-dev',
                'libboost1.53-dev', 'libboost-filesystem1.53-dev',
                'libboost-system1.53-dev', 'libgflags-dev',
                'libgoogle-glog-dev', 'git-core']
    print(_yellow('Installing dependancies..'))
    with settings(user='ubuntu', key_filename='~/eddy.pem'):
        sudo('apt-get -qq update')
        sudo('apt-get -y -qq dist-upgrade')
        sudo('apt-get -y -qq install %s' % ' '.join(packages))

@task
def ec2_deploy():
    """Deploy the newest VSFS on EC2.
    """
    conn = boto.ec2.connect_to_region(EC2_REGION)
    reservation = conn.get_all_instances()
    instance = reservation[0].instances[0]
    instance.update()

    while instance.state == u'pending':
        print(_yellow("Instance state: %s" % instance.state))
        time.sleep(10)
        instance.update()

    execute(ec2_install_packages, hosts=[instance.public_dns_name])
