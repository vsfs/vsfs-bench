#!/usr/bin/env python
#
# Helper utility for MR Log analytics.

from __future__ import print_function
import argparse
import os


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

    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    main()
