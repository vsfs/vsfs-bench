#!/usr/bin/env python
#
# Author: Lei Xu <eddyxu@gmail.com>

"""Help functions for mvd test.
"""

import argparse
import os
import shutil
import subprocess
import multiprocessing

ZIPF_FACTOR = 1.5

MVD = os.path.join(os.path.dirname(__file__), 'mvd')

def call_mvd(args, input_file, key):
    #print args, input_file
    input_filepath = os.path.join(args.base, input_file)
    cmd = '%s -duration %f -key %d -output mnt/foo/%s.out %s' % \
          (MVD, args.duration, key, input_file, input_filepath)
    output = subprocess.check_output(cmd, shell=True)

    if args.index:
        cmd = '../../vsbench -op record -driver %s -%s_host %s index.0 %s >/dev/null 2>/dev/null' % \
            (args.driver, args.driver, args.host, output)
        with open(os.devnull, "w") as fnull:
            subprocess.check_call(cmd, shell=True, stdout=fnull, stderr=fnull)


def run_mvd(args):
    # Open file list to count total inputs.
    with open(args.files) as fobj:
        files = [l.strip() for l in fobj]

    total_files = len(files)
    files_per_node = total_files / args.tasks
    begin = files_per_node * args.job
    end = min(begin + files_per_node, total_files)
    key = begin

    pool = multiprocessing.Pool(processes=8)
    for filepath in files[begin:end]:
        pool.apply_async(call_mvd, (args, filepath, key))
        key += 1
    pool.close()
    pool.join()


def create_directory(args):
    if os.path.exists(args.dir):
        shutil.rmtree(args.dir)
    os.makedirs(args.dir)

    format_str = '[00]%%0%dd.mol2' % len(str(args.subfiles))
    #print format_str
    random_buf = os.urandom(1024)
    for i in range(args.subfiles):
        # 4 - 512 KB zipf distribution.
        # file_size = min(3 + numpy.random.zipf(ZIPF_FACTOR), 512)
        file_size = 4
        file_name = os.path.join(args.dir, format_str % i)
        with open(file_name, 'w') as fobj:
            for s in range(file_size):
                fobj.write(random_buf)


def main():
    """MVD helper
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help')

    create_parser = subparsers.add_parser('create', help='create files.')
    create_parser.add_argument('-s', '--subfiles', type=int, default=100000,
                               metavar='NUM',
                               help='set number of files in a directory')
    create_parser.add_argument('-d', '--dist', choices=['zipf'],
                               help='set distribution of file size')
    create_parser.add_argument('--zipf', type=float, default=1.5,
                               metavar='NUM', help='set Zipf factor.')
    create_parser.add_argument('dir', metavar='DIR')
    create_parser.set_defaults(func=create_directory)

    mvd_parser = subparsers.add_parser('mvd', help='Run mvd')
    mvd_parser.add_argument('-d', '--duration', type=float, default=7.0,
                            help='set duration of mvd')
    mvd_parser.add_argument('-j', '--job', type=int, default=0,
                            help='set the job ID of this MVD program.')
    mvd_parser.add_argument('-f', '--files',
                            help='sets the file list for input')
    mvd_parser.add_argument('-i', '--index', default='',
                            help='sets the index name. '
                            'If empty, then do not do file-index.')
    mvd_parser.add_argument('-t', '--tasks', type=int,
                            help='sets the number of tasks.')
    mvd_parser.add_argument('-b', '--base',
                            help='set base directory.')
    mvd_parser.add_argument('-p', '--process', type=int, default=1,
                            help='Sets to use multiple process to run.')
    mvd_parser.add_argument('--driver',
                            help='set the driver to insert records.')
    mvd_parser.add_argument('--host', help='set the host of driver.')
    mvd_parser.set_defaults(func=run_mvd)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
