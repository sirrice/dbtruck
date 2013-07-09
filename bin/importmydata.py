#!/usr/bin/env python
import logging

from dbtruck.dbtruck import *
from dbtruck.util import get_logger
from dbtruck.exporters.pg import PGMethods
from dbtruck.exporters.csvmethods import CSVMethods

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--err', dest='errfile', default='/tmp/errlines.txt',
                        help='destination to log rows thare have errors')
    parser.add_argument('--log', dest='logfile', default='./log.txt',
                        help='destination to log debug messages')
    parser.add_argument('--append', action='store_true', dest='append', default=False,
                        help='replace existing table or append to it')
    parser.add_argument('--user', dest='user', default="sirrice",
                        help='database user (if applicable)')
    parser.add_argument('--port', dest='port', default=5432,
                        help='database port (if applicable)')
    parser.add_argument('-d', action='store_true', dest='debug', default=False,
                        help='print debug messages to stdout')
    parser.add_argument('tname', nargs=1, help='tablename')
    parser.add_argument('dbname', nargs=1, help='database name', default="test")
    parser.add_argument('fnames', nargs='+', help='data file name')
    args = parser.parse_args()

    tablename, dbname, fnames = args.tname[0], args.dbname[0], args.fnames
    dbuser = args.user
    dbport = args.port and int(args.port) or 5432 
    errfile = file(args.errfile, 'w')

    plevel = logging.DEBUG if args.debug else logging.WARNING
    _log = get_logger(fname=args.logfile, plevel=plevel)


    dbsettings = {
    'dbname' : dbname,
    'hostname' : 'localhost',
    'username' : dbuser,
    'port' : dbport,
    }


    import_datafiles(fnames, not args.append, tablename, errfile, PGMethods,
                     **dbsettings)
    errfile.close()

# potential options
# new = T/F
# errfile = ... file to print rows with errors
# 


