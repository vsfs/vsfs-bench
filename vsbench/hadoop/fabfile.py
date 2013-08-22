#!/usr/bin/env python
#
# Fabric configurations for HBase
#
# Author: Lei Xu <eddyxu@gmail.com>

"""start/stop HBase on Demand
"""

from fabric.api import parallel
from fabric.api import run, roles, env, execute, task, local, settings
from multiprocessing import Queue
from xml.dom import minidom
import os
import sys
import time
import yaml
sys.path.append('../..')
from vsbench.fablib import base_dir, download_tarball, create_indices
from vsbench import fablib

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
VERSION = '0.94.10'
#HADOOP_URL = 'http://www.trieuvan.com/apache/hadoop/common/stable/' + \
#             'hadoop-1.2.1.tar.gz'
HADOOP_URL = 'http://apache.petsads.us/hadoop/common/hadoop-2.0.5-alpha/' \
             'hadoop-2.0.5-alpha.tar.gz'

HBASE_URL = ("http://mirror.reverse.net/pub/apache/hbase/hbase-%s/" +
             "hbase-%s.tar.gz") % (VERSION, VERSION)
NODE_FILE = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', 'nodes.txt'))
VSBENCH = os.path.abspath('../vsbench')
RETRY = 10
DATA_DIR = '/scratch/datadir'
NAME_DIR = '/scratch/namedir'
ZOOKEEPER_DIR = '/scratch/zookeeper'


def load_config():
    """Initialize configurations.
    """
    if not 'JAVA_HOME' in os.environ:
        raise UserWarning("You must define JAVA_HOME in bash environment.")

    env.hadoop_dir = os.path.join(SCRIPT_DIR, base_dir(HADOOP_URL))
    env.hadoop_bin = os.path.join(env.hadoop_dir, 'bin')
    env.hadoop_sbin = os.path.join(env.hadoop_dir, 'sbin')
    env.hadoop_conf = os.path.join(env.hadoop_dir, 'conf')
    env.hbase_dir = os.path.join(SCRIPT_DIR, base_dir(HBASE_URL))
    env.hbase_bin = os.path.join(env.hbase_dir, 'bin')
    env.hbase_conf = os.path.join(env.hbase_dir, 'conf')

    if not os.path.exists(NODE_FILE):
        raise UserWarning("You must create a node list file {}"
                          .format(NODE_FILE))
    with open(NODE_FILE) as fobj:
        node_list = [line.strip() for line in fobj]
    env.head = node_list[0]
    env.workers = node_list[1:]

    env.roledefs = {}
    env.roledefs['head'] = [env.head]

    if os.path.exists('config.yaml'):
        global DATA_DIR, NAME_DIR, ZOOKEEPER_DIR
        user_configs = {}
        with open('config.yaml') as config_file:
            user_configs = yaml.load(config_file)
        DATA_DIR = user_configs.get('datadir', DATA_DIR)
        NAME_DIR = user_configs.get('namedir', NAME_DIR)
        ZOOKEEPER_DIR = user_configs.get('zookeeper_dir', ZOOKEEPER_DIR)


load_config()


def set_property_xml_node(dom, root, name, value):
    """Adds one property to the XML config file.
    """
    property_node = minidom.Element('property')
    name_node = minidom.Element('name')
    name_text = dom.createTextNode(name)
    name_node.appendChild(name_text)
    property_node.appendChild(name_node)
    value_node = minidom.Element('value')
    value_text = dom.createTextNode(value)
    value_node.appendChild(value_text)
    property_node.appendChild(value_node)
    root.appendChild(property_node)


def write_hadoop_config(filename, configs):
    """Write the configure file for hadoop/hbase.
    """
    dom = minidom.parseString('<configuration/>')
    inst = dom.createProcessingInstruction(
        'xml-stylesheet', 'type="text/xsl" href="configuration.xsl"')
    # pylint: disable=E1103
    root = dom.firstChild
    # pylint: enable=E1103
    dom.insertBefore(inst, root)

    for name, value in configs:
        set_property_xml_node(dom, root, name, value)

    with open(filename, 'w') as fobj:
        fobj.write(dom.toprettyxml())


def set_hdfs_cluster(num_datanodes, **kwargs):
    """Sets up the configurations for a HDFS cluster with num_datanodes
    DataNodes.
    """

    hbase = kwargs.get('hbase', True)
    hadoop_conf_dir = os.path.join(env.hadoop_dir, 'conf')
    hbase_conf_dir = os.path.join(env.hbase_dir, 'conf')

    if not os.path.exists(hadoop_conf_dir):
        os.makedirs(hadoop_conf_dir)

    # MasterNode address
    with open(os.path.join(hadoop_conf_dir, 'master'), 'w') as fobj:
        fobj.write('%s\n' % env.head)

    # DataNode address
    with open(os.path.join(hadoop_conf_dir, 'slaves'), 'w') as fobj:
        for node in env.workers[:num_datanodes]:
            fobj.write('%s\n' % node)

    write_hadoop_config(os.path.join(hadoop_conf_dir, 'hdfs-site.xml'),
                        [('dfs.namenode.name.dir', NAME_DIR),
                         ('dfs.datanode.data.dir', DATA_DIR)])

    write_hadoop_config(os.path.join(hadoop_conf_dir, 'core-site.xml'),
                        [('fs.defaultFS', 'hdfs://%(head)s/' % env)])

    if not hbase:
        return

    write_hadoop_config(os.path.join(hbase_conf_dir, 'hbase-site.xml'),
                        [('hbase.rootdir', 'hdfs://%(head)s/' % env),
                         ('hbase.cluster.distributed', 'true'),
                         ('hbase.client.scanner.caching', '1000'),
                         ('hbase.client.scanner.timeout.period', '3600000'),
                         ('hbase.regionserver.lease.period', '3600000'),
                         ('hbase.zookeeper.quorum', env.head),
                         ('hbase.zookeeper.property.dataDir', ZOOKEEPER_DIR),
                         ])

    with open(os.path.join(hbase_conf_dir, 'regionservers'), 'w') as fobj:
        for node in env.workers[:num_datanodes]:
            fobj.write('%s\n' % node)


@parallel
def prepare_directory():
    """Make directories for namenode and datanode.
    """
    if env.host == env.head:
        run('rm -rf %s' % NAME_DIR)
        run('mkdir -p %s' % NAME_DIR)
        run('chmod 755 %s' % NAME_DIR)
    else:
        run('rm -rf %s' % DATA_DIR)
        run('mkdir -p %s' % DATA_DIR)
        run('chmod 755 %s' % DATA_DIR)


@task
def download():
    execute(download_tarball, HADOOP_URL)
    execute(download_tarball, HBASE_URL)


@parallel
def start_datanode():
    run('%(hadoop_sbin)s/hadoop-daemon.sh --config %(hadoop_conf)s '
        '--script hdfs start datanode' % env)

@parallel
def start_nodemanager():
    run('%(hadoop_sbin)s/yarn-daemon.sh --config %(hadoop_conf)s '
        'start nodemanager' % env)

@task
@roles('head')
def start(nodes, **kwargs):
    """start(node): Starts a HBase cluster.

    @param nodes the number of nodes.
    @param hbase=True Also run hbase, default is true.
    """
    num_datanodes = int(nodes)

    hbase = False
    if 'hbase' in kwargs:
        hbase = True

    ret = execute(download_tarball, HADOOP_URL)
    if ret['<local-only>']:
        with open(os.path.join(env.hadoop_conf, 'hadoop-env.sh'), 'a') as fobj:
            fobj.write('export JAVA_HOME=%s\n' % os.environ['JAVA_HOME'])

    if hbase:
        ret = execute(download_tarball, HBASE_URL)
        if ret['<local-only>']:
            with open(os.path.join(env.hbase_conf,
                                   'hbase-env.sh'), 'a') as fobj:
                fobj.write('export JAVA_HOME=%s\n' % os.environ['JAVA_HOME'])

    set_hdfs_cluster(num_datanodes, **kwargs)
    execute(prepare_directory, hosts=[env.head])
    execute(prepare_directory, hosts=env.workers[:num_datanodes])
    run('yes Y | HADOOP_CONF_DIR=%(hadoop_conf)s '
        '%(hadoop_bin)s/hdfs namenode -format vsfs' % env)
    run('%(hadoop_sbin)s/hadoop-daemon.sh --config %(hadoop_conf)s '
        '--script hdfs start namenode' % env)
    execute(start_datanode, hosts=env.workers[:num_datanodes])
    run('%(hadoop_sbin)s/yarn-daemon.sh --config %(hadoop_conf)s '
        'start resourcemanager' % env)
    execute(start_nodemanager, hosts=env.workers[:num_datanodes])

    if hbase:
        run('%(hbase_bin)s/start-hbase.sh' % env)
        run('%(hbase_bin)s/hbase-daemon.sh start thrift' % env)


@parallel
def stop_datanode():
    run('%(hadoop_sbin)s/hadoop-daemon.sh --config %(hadoop_conf)s '
        '--script hdfs stop datanode' % env)

@parallel
def stop_nodemanager():
    run('%(hadoop_sbin)s/yarn-daemon.sh --config %(hadoop_conf)s '
        'stop nodemanager' % env)

@task
@roles('head')
def stop(**kwargs):
    """Stop a HBase cluster.
    """
    hbase = kwargs.get('hbase', True)
    if hbase:
        run("%(hbase_bin)s/hbase-daemon.sh stop thrift" % env)
        run("%(hbase_bin)s/stop-hbase.sh" % env)
    run('%(hadoop_sbin)s/hadoop-daemon.sh --config %(hadoop_conf)s '
        '--script hdfs stop namenode' % env)
    execute(stop_datanode, hosts=env.workers)
    run('%(hadoop_sbin)s/yarn-daemon.sh --config %(hadoop_conf)s '
        'stop resourcemanager' % env)
    execute(stop_nodemanager, hosts=env.workers)


@task
def hbase_dir():
    """Show hbase directory.
    """
    print base_dir(HBASE_URL)


@roles('head')
def init_hbase(num_indices):
    create_indices('hbase', num_indices)


def insert_records(slaves, indices, records, **kwargs):
    in_q = Queue()
    local('sleep 10')
    execute(start, slaves)
    execute(init_hbase, indices)
    num_clients = min(indices, 5)
    client_nodes = env.workers[-num_clients:]
    idx_per_client = indices / len(client_nodes)
    for i in range(len(client_nodes)):
        in_q.put('%d-%d' % (0 + i * idx_per_client,
                 min(indices, i * idx_per_client)))

    start_time = time.time()
    with settings(warn_only=True):
        execute(fablib.insert_record_worker, 'hbase', in_q, records,
                hosts=client_nodes)
    end_time = time.time()

    search_latencies = fablib.search('hbase', '/foo/bar', 'index.0', records)
    execute(stop)
    return "%d %d %0.2f %s" % (records * indices, slaves,
                               end_time - start_time, search_latencies)


@task
def test_scale(**kwargs):
    """Testing the scalability of HBase cluster.

    Keyword options:
    @param repeat the repeat time of running each iteration (default:3).
    @param indices the number of indices (default:50).
    @param records the records in each indices (default:100000).
    """
    repeat = int(kwargs.get('repeat', 3))
    indices = int(kwargs.get('indices', 50))
    records_per_index = int(kwargs.get('records', 100000))
    execute(stop)
    with open('test_scale.txt', 'w') as result_file:
        result_file.write('# Records servers time 10% 30% 50% iteration.\n')
        for slaves in range(2, 17, 2):
            for iteration in range(repeat):
                output = insert_records(slaves, indices, records_per_index)
                result_file.write("%s %d\n" % (output, iteration))
                result_file.flush()
                os.fsync(result_file.fileno())


@task
def test_search(**kwargs):
    repeat = int(kwargs.get('repeat', 1))
    records = int(kwargs.get('records', 10000000))

    execute(stop)
    with open('test_search.txt', 'w') as result_file:
        result_file.write('# Records servers time iteration\n')
        result_file.flush()
        for slaves in range(2, 17, 2):
            for iteration in range(repeat):
                output = insert_records(slaves, 1, records)
                result_file.write("%s %d\n" % (output, iteration))
                result_file.flush()
                os.fsync(result_file.fileno())