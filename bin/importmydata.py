#!python 
from dbtruck.dbtruck import *


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='')
    parser.add_argument('fname', nargs=1, help='data file name')
    parser.add_argument('tname', nargs=1, help='tablename')
    parser.add_argument('dbname', nargs=1, help='database name')
    parser.add_argument('--err', dest='errfile', default='/tmp/errlines.txt',
                        help='destination to log rows thare have errors')
    parser.add_argument('--append', action='store_true', dest='append', default=False,
                        help='new')
    args = parser.parse_args()

    fname, tablename, dbname = args.fname[0], args.tname[0], args.dbname[0]
    
    errfile = file(args.errfile, 'w')
    
    pgmethods = PGMethods(tablename, dbname, errfile)
    rowiter, types = setup(fname, not args.append, pgmethods)
    import_iterator(rowiter, types, pgmethods)
    errfile.close()

# potential options
# new = T/F
# errfile = ... file to print rows with errors
# 


