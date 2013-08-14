#!/usr/bin/env python
#
# Fabfile to start/stop MySQL Cluster.
#
# Author: Lei Xu <eddyxu@gmail.com>

"""Install and run MySQL Cluster.
"""
from __future__ import print_function
from fabric.api import local, run, roles, env, execute, task, cd, settings
from fabric.api import parallel
from multiprocessing import Queue
import fabric
import os
import shutil
import socket
import sys
sys.path.append('..')
from fablib import base_dir, download_tarball
import time

SCRIPT_DIR = os.path.dirname(__file__)
MAX_RETRY = 10
URL = "http://cdn.mysql.com/Downloads/MySQL-Cluster-7.3/" + \
    "mysql-cluster-gpl-7.3.2-linux-glibc2.5-x86_64.tar.gz"
VSBENCH = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, 'vsbench'))


def load_config():
    """Initialize configurations.
    """
    env.mysql_dir = base_dir(URL)
    env.bin_dir = os.path.join(env.mysql_dir, 'bin')
    env.data_dir = os.path.abspath('mysql_data')
    env.config_dir = os.path.abspath('mysql_config')

    with open('../nodes.txt') as fobj:
        node_list = [line.strip() for line in fobj]
    env.head = node_list[0]
    env.workers = node_list[1:]
    env.head_node_id = len(env.workers) + 2
    env.head_ip = socket.gethostbyname(env.head)

    env.roledefs = {}
    env.roledefs['head'] = [env.head]

load_config()


def set_config_files(num_nodes):
    """Sets the configure files for worker nodes.
    """
    if not os.path.exists(env.config_dir):
        os.makedirs(env.config_dir)

    with open(os.path.join(env.config_dir, "config.ini"), "w") as config_file:
        config_file.write("""
[ndbd default]
NoOfReplicas=1
DataMemory=2048M
IndexMemory=2048M
MaxNoOfConcurrentOperations=100000

[ndb_mgmd]
NodeId=1
hostname=%(head)s
datadir=%(data_dir)s

[mysqld]
NodeId=%(head_node_id)d
hostname=%(head)s
""" % env)

        for node_id in range(1, num_nodes + 1):
            data_dir = os.path.join(env.data_dir, str(node_id))
            node = env.workers[node_id - 1]
            config_file.write("""
[ndbd]
NodeId=%d
hostname=%s
datadir=%s
""" % (node_id + 1, node, data_dir))

            if os.path.exists(data_dir):
                shutil.rmtree(data_dir)
            os.makedirs(data_dir)


@roles('head')
def start_mysqld():
    """Starts mysql daemon
    """
    with open(os.path.join(env.config_dir, 'my.cnf'), 'w') as config:
        config.write("""
[client]
socket = %(data_dir)s/run/mysql.sock
port = 3306

[mysqld]
ndbcluster
user=%(user)s
datadir=%(data_dir)s
basedir=%(mysql_dir)s
#bind-address=0.0.0.0
pid-file=%(data_dir)s/run/mysqld.pid
socket=%(data_dir)s/run/mysql.sock
log_error=%(data_dir)s/error.log
character_set_server=utf8
collation_server=utf8_general_ci
#skip-character-set-client-handshake
max_connections=8000
""" % env)

    run_dir = os.path.join(env.data_dir, 'run')
    if os.path.exists(run_dir):
        shutil.rmtree(run_dir)
    os.makedirs(run_dir)
    #cmd = "nohup %s --defaults-file=%s </dev/null &>/dev/null &" % (
    cmd = "screen -d -m %s --defaults-file=%s" % (
        os.path.join(env.bin_dir, 'mysqld_safe'),
        os.path.join(env.config_dir, 'my.cnf'))
    with cd(env.mysql_dir):
        run(cmd, pty=False)


@roles('head')
def start_ndb_mgmd():
    """Starts NDB management daemon.
    """
    cmd = "%(bin_dir)s/ndb_mgmd --initial -f %(config_dir)s/config.ini " \
        "--configdir=%(config_dir)s"
    run(cmd % env)


def start_ndbd():
    """Starts NDBD
    """
    cmd = "%(bin_dir)s/ndbd -c %(head)s:1186" % env
    run(cmd)


@roles('head')
def setup_mysql_accounts():
    """Sets up database and tables for VSFS tests.
    """
    retry = MAX_RETRY
    init_sql_script = os.path.join("%(config_dir)s" % env, "init.sql")
    with settings(warn_only=True):
        with open(init_sql_script, 'w') as init_sql:
            init_sql.write("""use mysql
GRANT ALL ON *.* to root@'%';
FLUSH PRIVILEGES;
set global max_connections = 40000; """)
        while retry:
            output = run('mysql --socket=%s/run/mysql.sock -u root '
                         'mysql < %s' % (env.data_dir, init_sql_script))
            if not output.failed:
                break
            print("Retry mysql setup..{}".format(etry))
            retry -= 1
            local('sleep 5')


def _start_task(num_servers):
    """Starts a MySQL cluster with num_servers.
    """
    num_servers = int(num_servers)
    download_tarball(URL)
    set_config_files(num_servers)

    # Initialize MySQL database
    cmd = "%(mysql_dir)s/scripts/mysql_install_db --basedir=%(mysql_dir)s " + \
        "--no-defaults --datadir=%(data_dir)s"
    local(cmd % env)

    execute(start_ndb_mgmd)

    workers = env.workers[:num_servers]
    execute(start_ndbd, hosts=workers)
    execute(start_mysqld)

    local('sleep 10')
    execute(setup_mysql_accounts)


@task
def start(num_servers):
    """Starts a MySQL cluster.
    """
    _start_task(num_servers)


@task
@roles('head')
def stop():
    """Stops the MySQL cluster.
    """
    with settings(warn_only=True):
        run('%(bin_dir)s/mysqladmin --socket=%(data_dir)s/run/mysql.sock '
            '-u root shutdown' % env)
        run('%(bin_dir)s/ndb_mgm -e shutdown' % env)
        run('screen -wipe')


def _show_processes():
    """ Show mysql process on remote machine.
    """
    run('ps aux | grep mysql | grep -v grep || true')


@task
def status():
    """Query the status of the test cluster.
    """
    node_list = list(env.workers)
    node_list.append(env.head)
    fabric.state.output['running'] = False
    fabric.state.output['stderr'] = False
    execute(_show_processes, hosts=node_list)


@roles('head')
def init_db():
    """Populate database content.
    """
    run('%s -driver mysql -mysql_host %s -op create_indices '
        ' -num_indices 100 -mysql_schema single' % (VSBENCH, env.head))


@parallel
def run_mysql_client(in_q, records):
    """Run mysql client
    """
    idx_range = in_q.get()
    cmd = '%s -driver mysql -mysql_host %s -op insert -indices %s ' \
          '-records_per_index %d -cal_prefix -mysql_schema single' % \
        (VSBENCH, env.head, idx_range, records)
    print(cmd)
    run(cmd, stderr=sys.stderr, stdout=sys.stdout)


def run_mysql_search(query):
    cmd = '%s -driver mysql -mysql_host %s -op search -query "%s"' % \
        (VSBENCH, env.head, query)
    run(cmd)


def search(root, name, num_records):
    """Test search query.

    Search results for 10%, 30%, 50% results.

    @param root the root path to search.
    @param name the index name to search.
    @param num_records the number of records in each index.
    @return a string of "<latency> <latency> <latency>" for 10%, 30%, 50%
    results.
    """
    ret = ""
    start_key = int(num_records * 0.1)
    query = '%s/?%s>%d&%s<%d'
    for percent in [0.1, 0.3, 0.5]:
        start_time = time.time()
        end_key = int(start_key + percent * num_records)
        execute(run_mysql_search, query % (root, name, start_key,
                                           name, end_key),
                host=env.workers[0])
        end_time = time.time()
        ret += "%0.2f " % (end_time - start_time)

    return ret


@roles('head')
def import_files(num_files):
    """
    """
    cmd = '%s -driver mysql -mysql_host %s -op import ' \
          '-records_per_index %d' % \
          (VSBENCH, env.head, num_files)
    print(cmd)
    run(cmd)


def insert_records(slaves, indices, records):
    """Insert records into MySQL.

    @param slaves the number of slave nodes.
    @param indices the number of indices.
    @param records the number of records for each index.
    @return a string of "<total_records> <slaves> <latency>"
    """
    num_slaves = int(slaves)
    num_indices = int(indices)
    num_records = int(records)
    execute(stop)
    in_q = Queue()
    local('sleep 10')
    _start_task(num_slaves)
    execute(init_db)
    execute(import_files, num_records)
    client_nodes = env.workers[-5:]
    idx_per_client = num_indices / len(client_nodes)
    for i in range(len(client_nodes)):
        in_q.put('%d-%d' % (0 + i * idx_per_client,
                 min(num_indices, (i + 1) * idx_per_client) - 1))

    start_time = time.time()
    with settings(warn_only=True):
        execute(run_mysql_client, in_q, num_records, hosts=client_nodes)
    end_time = time.time()
    search_output = search('/foo/bar', 'index.0',
                           num_records)

    execute(stop)
    return "%d %d %0.2f %s" % (num_records * num_indices, num_slaves,
                               end_time - start_time, search_output)


@task
def test_insert(slaves, indices, records, **kwargs):
    """Test insert performance for one iteration (param: slaves,\
indices,records).
    """
    print(insert_records(slaves, indices, records))
    sys.stdout.flush()


@task
def test_scale(**kwargs):
    """Testing the scalability of MySQL cluster.

    Keyword parameters:
    repeat -- set the repeat time of each test configuration (3).
    indices -- sets the total number of indices (50).
    records -- sets the number of records in each index (100000).
    """
    repeat = int(kwargs.get('repeat', 3))
    indices = int(kwargs.get('indices', 50))
    records_per_index = int(kwargs.get('records', 100000))
    execute(stop)  # clean up all existing cluster first.
    with open('test_scale.txt', 'w') as result_file:
        result_file.write('# Records servers time 10% 30% 50% iteration.\n')
        for slaves in range(2, 17, 2):
            for iteration in range(repeat):
                output = insert_records(slaves, indices, records_per_index)
                result_file.write("%s %d\n" % (output, iteration))
                result_file.flush()
                os.fsync(result_file.fileno())
