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


def test_insert(args):
    """Test inserting benchmark
    """
    def mpirun(args):
        if args.mpi:
            cmd = 'mpirun --mca orte_base_help_aggregate 0 '
        else:
            cmd = 'srun '
        cmd += '%s -driver %s' \
               ' -%s_host %s -%s_port %d -op insert ' \
               '-num_indices 2 -records_per_index %d' % \
               (VSBENCH, args.driver, args.driver, fabfile.env['head'],
                args.driver, fabfile.MONGOS_PORT, args.total / 63)
        if args.mpi:
            cmd += ' -mpi_barrier'
        print(cmd)
        check_output(cmd, shell=True)

    destory_cluster()
    time.sleep(3)
    for shard in [1]:  # range(2, 18, 4):
        prepare_cluster(shard)
        time.sleep(3)
        print('Import files.', file=sys.stderr)
        check_output('srun %s -driver mongodb -mongodb_host %s -mongodb_port %d'
                     ' -op import -records_per_index %d' %
                     (VSBENCH, fabfile.env['head'], fabfile.MONGOS_PORT,
                     args.total / 63),
                     shell=True)
        print('Run insert for %d shard' % shard, file=sys.stderr)
        start_time = time.time()
        mpirun(args)
        end_time = time.time()
        print('%d %0.2f' % (shard, end_time - start_time))
        destory_cluster()


def main():
    """Main function
    """
    parser = argparse.ArgumentParser(
        usage='sbatch -n NUM_CLIENTS %(prog)s [options] TEST',
        description='Run VSFS benchmark on sandhills (SLURM).')
    parser.add_argument('-d', '--driver', default='mongodb',
                        choices=['mongodb', 'hbase', 'mysql'])
    subparsers = parser.add_subparsers(help='Available tests')

    parser_insert = subparsers.add_parser('insert',
                                          help='test inserting performance')
    parser_insert.add_argument('-t', '--total', type=int, default=10**7,
                               help='Total number of index records.')
    parser_insert.add_argument('--mpi', action="store_true", default=False,
                               help='Use MPI to synchronize clients.')
    parser_insert.set_defaults(func=test_insert)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
