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
            local('fab start:%d,%d' % (num_shard, num_shard))
        else:
            local('fab start:%d' % num_shard)


def destory_cluster(driver):
    print(red('Shutting down the cluster.'), file=sys.stderr)
    with settings(warn_only=True), lcd(os.path.join(SCRIPT_DIR, driver)):
        local('fab stop')


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


def test_search(args):
    args.output.write("# Shard Latency\n")
    num_files = args.nfiles
    shard_confs = map(int, args.shards.split(','))
    destory_cluster(args.driver)
    time.sleep(3)
    for shard in shard_confs:
        prepare_cluster(args.driver, shard)
        time.sleep(10)
        print(yellow('Populating namespace...'), file=sys.stderr)
        print(yellow('Importing files...'), file=sys.stderr)
        check_output('srun -n 10 %s -driver %s -%s_host %s '
                     '-op import -records_per_index %d' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                      num_files), shell=True)
        print(yellow('Building 2 indices, each %d records..' % num_files),
              file=sys.stderr)
        check_output('%s -driver %s -%s_host %s -op create_indices '
                     '-num_indices 100' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head']),
                     shell=True)
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
        print(yellow('Populating namespace...'), file=sys.stderr)
        print(yellow('Importing files...'), file=sys.stderr)
        parallel_run('-op import -records_per_index 100000')
        print(yellow('Building 100 indices, each 100,000 records..'),
              file=sys.stderr)
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
        description='run VSFS benchmark on sandhills (SLURM).')
    parser.add_argument('-d', '--driver', metavar='NAME', default='mongodb',
                        choices=avail_drivers(),
                        help='available drivers: %(choices)s')
    parser.add_argument(
        '-s', '--shards', metavar='N0,N1,N2..',
        default=','.join(map(str, range(2, 21, 2))),
        help='Comma separated string of the numbers of shared servers to '
        'test against (default: "%(default)s").')
    parser.add_argument('--mpi', action="store_true", default=False,
                        help='use MPI to synchronize clients.')
    parser.add_argument('-o', '--output', type=argparse.FileType('w'),
                        default='slurm_results.txt', metavar='FILE',
                        help='set output file (default: stdout)')

    subparsers = parser.add_subparsers(help='Available tests')

    parser_index = subparsers.add_parser(
        'index', help='test indexing performance')
    parser_index.add_argument(
        '-t', '--total', type=int, default=10**7, metavar='NUM',
        help='Total number of index records (default: %(default)d).')
    parser_index.add_argument(
        '-i', '--index', type=int, default=63, metavar='NUM',
        help='Number of indices')
    parser_index.add_argument('--id')
    parser_index.set_defaults(func=test_index)

    parser_search = subparsers.add_parser(
        'search', help='test searching performance')
    parser_search.add_argument(
        '-n', '--nfiles', type=int, default=100000, metavar='NUM',
        help='set number of files (default: %(default)d)')
    parser_search.set_defaults(func=test_search)

    parser_open_search = subparsers.add_parser(
        'open_search', help='test searching in open loop.')
    parser_open_search.add_argument(
        '-n', '--nfiles', type=int, default=100000, metavar='NUM',
        help='set number of files (default: %(default)d)')
    parser_open_search.set_defaults(func=test_open_search)

    args = parser.parse_args()

    module_name = 'vsbench.%s.fabfile' % args.driver
    fabfile = importlib.import_module(module_name)
    args.func(args)
