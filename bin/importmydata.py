#!/usr/bin/env python
import logging

from dbtruck.dbtruck import *
from dbtruck.util import get_logger
from dbtruck.exporters.pg import PGMethods

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('fname', nargs=1, help='data file name')
    parser.add_argument('tname', nargs=1, help='tablename')
    parser.add_argument('dbname', nargs=1, help='database name')
    parser.add_argument('--err', dest='errfile', default='/tmp/errlines.txt',
                        help='destination to log rows thare have errors')
    parser.add_argument('--log', dest='logfile', default='./log.txt',
                        help='destination to log debug messages')
    parser.add_argument('--append', action='store_true', dest='append', default=False,
                        help='replace existing table or append to it')
    parser.add_argument('-d', action='store_true', dest='debug', default=False,
                        help='print debug messages to stdout')
    args = parser.parse_args()

    fname, tablename, dbname = args.fname[0], args.tname[0], args.dbname[0]
    errfile = file(args.errfile, 'w')

    plevel = logging.DEBUG if args.debug else logging.WARNING
    _log = get_logger(fname='foo.txt', plevel=plevel)


    import_datafiles(fname, not args.append, tablename, dbname, errfile, PGMethods)
    errfile.close()

# potential options
# new = T/F
# errfile = ... file to print rows with errors
# 


