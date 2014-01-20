#!/usr/bin/env python
#
# Copyright 2013 (c) Lei Xu <eddyxu@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
from collections import defaultdict
import argparse
import os
from subprocess import Popen, PIPE, call, check_output
from multiprocessing import Pool

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
VSFS_UTIL = os.path.join(SCRIPT_DIR, '../../lib/vsfs/vsfs/ui/cli/vsfs')


def extract_from_file(args):
    filepath, threshold = args
    csvfile = os.path.basename(filepath)
    with open(filepath) as fobj:
        #print("processing {}".format(filepath))
        max_value = defaultdict(float)
        for line in fobj:
            line = line.strip()
            fields = line.split(',')
            if len(fields) < 5 or not fields[4]:
                continue
            name = fields[3]
            value = float(fields[4])
            max_value[name] = max(max_value[name], value)
        if threshold:
            if max_value['Writer_5_runtime'] >= threshold:
                print(csvfile)
        else:
            if max_value['Writer_5_runtime'] > 0:
                print(csvfile, max_value['Writer_5_runtime'])
    return (csvfile, max_value)


def extract_features(args):
    """Extract intersting features
    """
    subprocess_args = []
    for csvfile in os.listdir(args.csvdir):
        path = os.path.join(args.csvdir, csvfile)
        subprocess_args.append((path, args.threshold))

    pool = Pool()
    count = 0
    with open('features.txt', 'w') as fobj:
        for csvfile, max_values in pool.imap(extract_from_file,
                                             subprocess_args):
            for name, value in max_values.items():
                fobj.write('%s %s %f\n' % (csvfile, name, value))
            count += 1
            if count % 50 == 0:
                fobj.flush()


def run_index(args):
    feature, values = args
    cmd = '%s index create %s -k float -t btree /test %s' % \
            (VSFS_UTIL, feature)
    call(cmd, shell=True)
    cmd = '%s index insert -s n %s' % (VSFS_UTIL, feature)
    content = ""
    for csvfile, value in values.items():
        content += "/test/%s %s\n" % (csvfile, value)
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = p.communicate(input=content)
    print(stdout)
    print(stderr)


def index_features(args):
    records = defaultdict(dict)
    for line in args.file:
        fields = line.split()
        filename, feature, value = fields
        records[feature][filename] = value

    pool = Pool(processes=8)
    pool.map(run_index, records.items())


def import_namespace(args):
    if not os.path.exists(args.tardir):
        os.makedirs(args.tardir)
    for root, dirs, files in os.walk(args.srcdir):
        for filename in files:
            path = os.path.join(root, filename)
            relpath = os.path.relpath(path, args.srcdir)
            path_in_vsfs = os.path.join(args.tardir, relpath)
            if args.verbose:
                print(path_in_vsfs)
            with open(path_in_vsfs, 'w') as fobj:
                pass


def main():
    """MapReduce Log Analytic helper
    """
    parser = argparse.ArgumentParser(
        description='MapReduce log analytic helper')
    parser.add_argument('--verbose', action='store_true', default=False,
                        help='run in verbose mode')
    subparsers = parser.add_subparsers()

    parser_import = subparsers.add_parser('import')
    parser_import.add_argument('srcdir')
    parser_import.add_argument('tardir')
    parser_import.set_defaults(func=import_namespace)

    parser_extract = subparsers.add_parser('extract')
    parser_extract.add_argument(
        '-t', '--threshold', type=int, metavar='NUM', default=0,
        help='Sets the threshold to print filename')
    parser_extract.add_argument('csvdir')
    parser_extract.set_defaults(func=extract_features)

    parser_index = subparsers.add_parser('index')
    parser_index.add_argument('-H', '--host', default='localhost',
                              help='set the master node address.')
    parser_index.add_argument('-p', '--prefix', default='/test',
                              help='set the prefix of directory to index.')
    parser_index.add_argument('file', type=argparse.FileType('r'))
    parser_index.set_defaults(func=index_features)

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
