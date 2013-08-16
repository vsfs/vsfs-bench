#!/usr/bin/env python
#SBATCH --mem-per-cpu=1024
#SBATCH --partition=guest
#SBATCH --error=job.%J.err
#SBATCH --output=job.%J.out
#SBATCH --cpus-per-task=1
#
# Author: Lei Xu <eddyxu@gmail.com>
#
# TODO(eddyxu): generalize this to all drivers

from subprocess import check_output
import argparse
import os
import time
import fabfile

SCRIPT_DIR = os.path.abspath(os.curdir)
print(SCRIPT_DIR)
VSBENCH = os.path.join(SCRIPT_DIR, os.pardir, 'bin/vsbench')
FABFILE = os.path.join(SCRIPT_DIR, 'fabfile.py')
print(fabfile.__file__)

def get_node_list():
    node_list_str = os.getenv('SLURM_NODELIST')
    print(node_list_str)


def prepare_cluster(num_shard):
    """
    """
    check_output('fab -f %s start:%d' % (FABFILE, num_shard), shell=True)


def destory_cluster():
    check_output('fab -f %s stop' % (FABFILE), shell=True)


def test_insert(args):
    """Test inserting benchmark
    """
    def mpirun(args):
        cmd = 'mpirun --mca orte_base_help_aggregate 0 %s -driver %s' \
              ' -%s_host %h -%s_port %d' % \
                (VSBENCH, args.driver, args.driver, args.driver, 27018)

    destory_cluster()
    for shard in range(2, 18, 4):
        prepare_cluster(shard)
        time.sleep(3)
        print(fabfile.env)
        print('Run insert for %d shard' % shard)
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
    parser_insert.set_defaults(func=test_insert)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
