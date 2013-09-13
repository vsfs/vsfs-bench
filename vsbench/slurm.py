#!/usr/bin/env python
#SBATCH --time=12:00:00
#SBATCH --mem-per-cpu=1024
#SBATCH --partition=guest
#SBATCH --error=job.%J.err
#SBATCH --output=job.%J.out
#SBATCH --cpus-per-task=1
#
# Author: Lei Xu <eddyxu@gmail.com>
#
# TODO(eddyxu): generalize this to all drivers

from __future__ import print_function
from subprocess import check_output
from fabric.api import lcd, local, settings
from fabric.colors import yellow, red
import argparse
import importlib
import os
import sys
import time
sys.path.append('..')

SCRIPT_DIR = os.path.abspath(os.curdir)
VSBENCH = os.path.abspath(os.path.join(SCRIPT_DIR, '..', 'bin/vsbench'))
FABFILE = os.path.join(SCRIPT_DIR, 'fabfile.py')
fabfile = None   # module


def prepare_cluster(driver, num_shard):
    """
    """
    print(yellow('Preparing cluster..'), file=sys.stderr)
    with settings(warn_only=True), lcd(os.path.join(SCRIPT_DIR, driver)):
        if driver == 'vsfs':
            #local('fab start:%d,%d' % (num_shard, num_shard))
            local('fab start:%d,%d' % (2, num_shard))
        else:
            local('fab start:%d' % num_shard)


def destory_cluster(driver):
    print(red('Shutting down the cluster.'), file=sys.stderr)
    with settings(warn_only=True), lcd(os.path.join(SCRIPT_DIR, driver)):
        local('fab stop')


def populate_namesapce(driver, nfiles, nindices):
    """Populate the namespace with 'nfiles' files and 'nindices' index.
    @param driver the name of file search driver. (vsfs/mysql/voltdb..)
    @param nfiles number of files in the namespace.
    @param nindices number of indexes in the namespace.
    """
    print(yellow('Populating namespace...'), file=sys.stderr)
    print(yellow('Importing files...'), file=sys.stderr)
    check_output('srun -n 10 %s -driver %s -%s_host %s '
                 '-op import -records_per_index %d' %
                 (VSBENCH, driver, driver, fabfile.env['head'], nfiles / 10),
                 shell=True)
    print(yellow('Building %s indices...' % nindices),
          file=sys.stderr)
    check_output('%s -driver %s -%s_host %s -op create_indices '
                 '-num_indices %d' %
                 (VSBENCH, driver, driver, fabfile.env['head'], nindices),
                 shell=True)

def parallel_run(params, mpi=False, debug=False):
    """Parallelly running clients
    """
    if mpi:
        run_cmd = 'mpirun --mca orte_base_help_aggregate 0 '
    else:
        run_cmd = 'srun '

    run_cmd += ' %s -driver %s -%s_host %s ' % \
               (VSBENCH, args.driver, args.driver, fabfile.env['head'])
    run_cmd += params
    if mpi:
        run_cmd += ' -mpi'
    if debug:
        print(run_cmd, file=sys.stderr)
    print(run_cmd)
    check_output(run_cmd, shell=True)


def create_indices(args, num_indices):
    print(yellow('Intializing DB and creating indices...'), file=sys.stderr)
    driver = args.driver
    cmd = '%s -driver %s -%s_host %s -op create_indices -num_indices %d' % \
          (VSBENCH, driver, driver, fabfile.env['head'], num_indices)
    if driver == 'mysql':
        cmd += ' -mysql_schema single'
    check_output(cmd, shell=True)


def test_index(args):
    """Test indexing performance
    """
    num_indices = 63  # Max indices supported in mongodb

    def run_test(args):
        """
        """
        params = '-op insert -num_indices 2 -records_per_index %d' % \
                 (args.total / num_indices)
        if args.driver == 'mysql':
            params += ' -cal_prefix -mysql_schema single'
        parallel_run(params, args.mpi)

    driver = args.driver
    args.output.write("# Shard\tTotal\tLatency\n")
    destory_cluster(args.driver)
    time.sleep(3)
    shard_confs = map(int, args.shards.split(','))
    for shard in shard_confs:
        prepare_cluster(args.driver, shard)
        time.sleep(3)
        if driver != 'mongodb':
            # MongoDB's indices are created when start the cluster. Calling
            # "vsbench -op create_indices" crahses the benchmark. Need to
            # investigate later.
            create_indices(args, num_indices)
        print('Importing files.', file=sys.stderr)
        params = '-op import -records_per_index %d' % \
                 (args.total / num_indices)
        parallel_run(params)
        print('Run insert for %d shard' % shard, file=sys.stderr)
        start_time = time.time()
        run_test(args)
        end_time = time.time()
        args.output.write('%d %d %0.2f\n' %
                          (shard, args.total, end_time - start_time))
        args.output.flush()
        destory_cluster(args.driver)


def test_open_index(args):
    """Use open loop to test the latency of VSFS.
    """
    #ntasks = int(os.environ.get('SLURM_NTASKS'))
    driver = 'vsfs'
    prepare_cluster(driver, 16)
    time.sleep(5)
    populate_namesapce(args.driver, args.total, 240)

    return
    print(yellow('Run insert in open loop'), file=sys.stderr)
    params = '%s -driver %s -%s_host %s -op insert ' \
             '-num_indices 2 -records_per_index %d -batch_size 1 -latency' % \
             (VSBENCH, driver, driver, fabfile.env['head'], args.total)
    parallel_run(params)


def test_search(args):
    args.output.write("# Shard Latency\n")
    num_files = args.nfiles
    shard_confs = map(int, args.shards.split(','))
    destory_cluster(args.driver)
    time.sleep(3)
    for shard in shard_confs:
        prepare_cluster(args.driver, shard)
        time.sleep(10)
        num_indices = 100
        populate_namesapce(args.driver, num_files, num_indices)
        check_output('srun -n 10 %s -driver %s -%s_host %s '
                     '-op insert -num_indices 10 -records_per_index %s' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                      num_files),
                     shell=True)
        start_time = time.time()
        search_cmd = '%s -driver %s -%s_host %s -op search -query "%s"' % \
                     (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                      "/foo/bar?index0>10000&index0<20000")
        print(search_cmd)
        check_output(search_cmd, shell=True)
        end_time = time.time()
        args.output.write('%d %0.2f\n' % (shard, end_time - start_time))
        args.output.flush()
        destory_cluster(args.driver)
    args.output.close()


def test_open_search(args):
    """Test search latency in open loop.
    """
    def run_test(args):
        """
        """
        parallel_run('-op open_search -num_indices 20', args.mpi)

    args.output.write("# Shard Latency\n")
    shard_confs = map(int, args.shards.split(','))
    destory_cluster()
    time.sleep(3)
    for shard in shard_confs:
        prepare_cluster(args.driver, shard)
        time.sleep(3)
        populate_namesapce(args.driver, 100000, 100)
        check_output('srun -n 10 %s -driver %s -%s_host %s '
                     '-op insert -num_indices 2 -records_per_index 50000' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head']),
                     shell=True)
        start_time = time.time()
        run_test(args)
        end_time = time.time()
        args.output.write('%d %0.2f\n' % (shard, end_time - start_time))
        args.output.flush()
        destory_cluster()
    args.output.close()


def avail_drivers():
    drivers = []
    for subdir in os.listdir(SCRIPT_DIR):
        if os.path.exists(os.path.join(subdir, 'fabfile.py')):
            drivers.append(subdir)
    return drivers


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        usage='sbatch -n NUM_CLIENTS %(prog)s [options] TEST',
        description='run VSFS benchmark on sandhills (SLURM).',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-d', '--driver', metavar='NAME', default='mongodb',
                        choices=avail_drivers(),
                        help='available drivers: %(choices)s')
    parser.add_argument(
        '-s', '--shards', metavar='N0,N1,N2..',
        default=','.join(map(str, range(2, 21, 2))),
        help='Comma separated string of the numbers of shared servers to '
        'test against')
    parser.add_argument('--mpi', action="store_true", default=False,
                        help='use MPI to synchronize clients.')
    parser.add_argument('-o', '--output', type=argparse.FileType('w'),
                        default='slurm_results.txt', metavar='FILE',
                        help='set output file')

    subparsers = parser.add_subparsers(help='Available tests')

    parser_index = subparsers.add_parser(
        'index', help='test indexing performance',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_index.add_argument(
        '-t', '--total', type=int, default=10**7, metavar='NUM',
        help='Total number of index records.')
    parser_index.add_argument(
        '-i', '--index', type=int, default=63, metavar='NUM',
        help='Number of indices')
    parser_index.add_argument('--id')
    parser_index.set_defaults(func=test_index)

    parser_open_index = subparsers.add_parser(
        'open_index', help='test indexing in open loop to measure latency',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_open_index.add_argument(
        '-b', '--batch', type=int, default=1, metavar='NUM',
        help='set the batch size')
    parser_open_index.add_argument(
        '-t', '--total', type=int, default=10**4, metavar='NUM',
        help='Set the number of records to index.'
    )
    parser_open_index.set_defaults(func=test_open_index)

    parser_search = subparsers.add_parser(
        'search', help='test searching performance',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_search.add_argument(
        '-n', '--nfiles', type=int, default=100000, metavar='NUM',
        help='set number of files.')
    parser_search.set_defaults(func=test_search)

    parser_open_search = subparsers.add_parser(
        'open_search', help='test searching in open loop to measure latency.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser_open_search.add_argument(
        '-n', '--nfiles', type=int, default=100000, metavar='NUM',
        help='set number of files.')
    parser_open_search.set_defaults(func=test_open_search)

    args = parser.parse_args()

    module_name = 'vsbench.%s.fabfile' % args.driver
    fabfile = importlib.import_module(module_name)
    args.func(args)
