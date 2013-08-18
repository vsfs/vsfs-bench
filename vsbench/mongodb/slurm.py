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
from fabric.colors import yellow, red
import argparse
import os
import sys
import time
sys.path.append('../..')
from vsbench.mongodb import fabfile

SCRIPT_DIR = os.path.abspath(os.curdir)
VSBENCH = fabfile.VSBENCH
FABFILE = os.path.join(SCRIPT_DIR, 'fabfile.py')


def prepare_cluster(num_shard):
    """
    """
    print(yellow('Preparing cluster..'), file=sys.stderr)
    check_output('fab -f %s start:%d' % (FABFILE, num_shard), shell=True)


def destory_cluster():
    print(red('Shutting down the cluster.'), file=sys.stderr)
    check_output('fab -f %s stop' % (FABFILE), shell=True)


def test_index(args):
    """Test indexing performance
    """
    num_indices = 63

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

    destory_cluster()
    time.sleep(3)
    shard_confs = map(int, args.shards.split(','))
    for shard in shard_confs:
        prepare_cluster(shard)
        time.sleep(3)
        print('Importing files.', file=sys.stderr)
        check_output('srun %s -driver mongodb -mongodb_host %s '
                     '-mongodb_port %d -op import -records_per_index %d' %
                     (VSBENCH, fabfile.env['head'], fabfile.MONGOS_PORT,
                      args.total / num_indices),
                     shell=True)
        print('Run insert for %d shard' % shard, file=sys.stderr)
        start_time = time.time()
        mpirun(args)
        end_time = time.time()
        print('%d %d %0.2f' % (shard, args.total, end_time - start_time))
        sys.stdout.flush()
        destory_cluster()


def test_search(args):
    """Test search performance
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

    destory_cluster()
    time.sleep(3)
    shard_confs = map(int, args.shards.split(','))
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
        check_output('srun -n 10 %s -driver mongodb -mongodb_host %s -op insert'
                     ' -num_indices 10 -records_per_index 100000' % \
                     (VSBENCH, fabfile.env['head']),
                     shell=True)
        start_time = time.time()
        mpirun(args)
        end_time = time.time()
        print('%d %0.2f' % (shard, end_time - start_time))
        sys.stdout.flush()
        destory_cluster()


def main():
    """Main function
    """
    parser = argparse.ArgumentParser(
        usage='sbatch -n NUM_CLIENTS %(prog)s [options] TEST',
        description='Run VSFS benchmark on sandhills (SLURM).')
    parser.add_argument('-d', '--driver', default='mongodb',
                        choices=['mongodb', 'hbase', 'mysql'])
    parser.add_argument(
        '-s', '--shards', metavar='N0,N1,N2..',
        default=','.join(map(str, range(2, 21, 2))),
        help='Comma separated string of the numbers of shared servers to '
        'test against (default: "%(default)s").')
    parser.add_argument('-o', '--output', default='', metavar='FILE',
                        help='set output file (default: stdout)')
    subparsers = parser.add_subparsers(help='Available tests')

    parser_index = subparsers.add_parser(
        'index', help='test indexing performance')
    parser_index.add_argument(
        '-t', '--total', type=int, default=10**7, metavar='NUM',
        help='Total number of index records (default: %(default)d).')
    parser_index.add_argument('--mpi', action="store_true", default=False,
                              help='Use MPI to synchronize clients.')
    parser_index.set_defaults(func=test_index)

    parser_search = subparsers.add_parser(
        'search', help='test searching performance')
    parser_search.set_defaults(func=test_search)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
