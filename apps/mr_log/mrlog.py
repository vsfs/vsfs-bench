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

VSFS_UTIL = os.path.join(os.path.dirname(__file__),
                         '../../lib/vsfs/vsfs/client/vsfs')


def extract_features(args):
    """Extract intersting features
    """
    results = {}
    for csvfile in os.listdir(args.csvdir):
        path = os.path.join(args.csvdir, csvfile)
        with open(path) as fobj:
            max_value = defaultdict(float)
            for line in fobj:
                line = line.strip()
                fields = line.split(',')
                if len(fields) < 5 or not fields[4]:
                    continue
                name = fields[3]
                value = fields[4]
                max_value[name] = max(max_value[name], value)
            if args.threshold:
                if max_value['Writer_5_runtime'] >= args.threshold:
                    print(csvfile)
            else:
                if max_value['Writer_5_runtime'] > 0:
                    print(csvfile, max_value['Writer_5_runtime'])

        results[csvfile] = max_value

    with open('features.txt', 'w') as fobj:
        for csvfile, max_values in results.items():
            for name, value in max_values.items():
                fobj.write('%s %s %f\n' % (csvfile, name, value))



def import_namespace(args):
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

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
