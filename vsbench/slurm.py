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
from fabric.api import lcd, local
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
    with lcd(os.path.join(SCRIPT_DIR, driver)):
        local('fab start:%d' % num_shard)


def destory_cluster(driver):
    print(red('Shutting down the cluster.'), file=sys.stderr)
    with lcd(os.path.join(SCRIPT_DIR, driver)):
        local('fab stop')


def test_index(args):
    """Test indexing performance
    """
    num_indices = 63  # Max indices supported in mongodb

    def mpirun(args):
        """
        """
        if args.mpi:
            cmd = 'mpirun --mca orte_base_help_aggregate 0 '
        else:
            cmd = 'srun '
        cmd += '%s -driver %s' \
               ' -%s_host %s -%s_port %d -op insert ' \
               '-num_indices 2 -records_per_index %d' % \
               (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                args.driver, fabfile.MONGOS_PORT, args.total / num_indices)
        if args.mpi:
            cmd += ' -mpi'
        print(cmd)
        check_output(cmd, shell=True)

    args.output.write("# Shard\tTotal\tLatency\n")
    destory_cluster(args.driver)
    time.sleep(3)
    shard_confs = map(int, args.shards.split(','))
    for shard in shard_confs:
        prepare_cluster(args.driver, shard)
        time.sleep(3)
        print('Importing files.', file=sys.stderr)
        check_output('srun %s -driver %s -%s_host %s '
                     '-op import -records_per_index %d' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                      args.total / num_indices),
                     shell=True)
        print('Run insert for %d shard' % shard, file=sys.stderr)
        start_time = time.time()
        mpirun(args)
        end_time = time.time()
        args.output.write('%d %d %0.2f\n' %
                          (shard, args.total, end_time - start_time))
        args.output.flush()
        destory_cluster(args.driver)


def test_search(args):
    args.output.write("# Shard Latency\n")
    num_files = args.nfiles
    shard_confs = map(int, args.shards.split(','))
    destory_cluster()
    time.sleep(3)
    for shard in shard_confs:
        prepare_cluster(shard)
        time.sleep(3)
        print(yellow('Populating namespace...'), file=sys.stderr)
        print(yellow('Importing files...'), file=sys.stderr)
        check_output(
            'srun %s -driver %s -%s_host %s -op import -records_per_index %s' %
            (VSBENCH, args.driver, args.driver, fabfile.env['head'],
             num_files), shell=True)
        print(yellow('Building 2 indices, each %d records..' % num_files),
              file=sys.stderr)
        check_output('srun -n 2 %s -driver %s -%s_host %s '
                     '-op insert -num_indices 1 -records_per_index %s' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                      num_files),
                     shell=True)
        start_time = time.time()
        check_output('%s -driver %s -%s_host %s -op search -query "%s"' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                      "/foo/bar?index0>10000&index0<20000"),
                     shell=True)
        end_time = time.time()
        args.output.write('%d %0.2f\n' % (shard, end_time - start_time))
        args.output.flush()
        destory_cluster()
    args.output.close()


def test_open_search(args):
    """Test search latency in open loop.
    """
    def mpirun(args):
        """
        """
        if args.mpi:
            cmd = 'mpirun --mca orte_base_help_aggregate 0 '
        else:
            cmd = 'srun '
        cmd += '%s -driver %s -%s_host %s -%s_port %d -op open_search' \
               ' -num_indices 20' % \
               (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                args.driver, fabfile.MONGOS_PORT)
        if args.mpi:
            cmd += ' -mpi'
        print(cmd, file=sys.stderr)
        check_output(cmd, shell=True)

    args.output.write("# Shard Latency\n")
    shard_confs = map(int, args.shards.split(','))
    destory_cluster()
    time.sleep(3)
    for shard in shard_confs:
        prepare_cluster(shard)
        time.sleep(3)
        print(yellow('Populating namespace...'), file=sys.stderr)
        print(yellow('Importing files...'), file=sys.stderr)
        check_output('srun %s -driver mongodb -mongodb_host %s '
                     '-mongodb_port %d -op import -records_per_index 100000' %
                     (VSBENCH, fabfile.env['head'], fabfile.MONGOS_PORT),
                     shell=True)
        print(yellow('Building 100 indices, each 100,000 records..'),
              file=sys.stderr)
        check_output('srun -n 10 %s -driver %s -%s_host %s '
                     '-op insert -num_indices 2 -records_per_index 50000' %
                     (VSBENCH, args.driver, args.driver, fabfile.env['head']),
                     shell=True)
        start_time = time.time()
        mpirun(args)
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
