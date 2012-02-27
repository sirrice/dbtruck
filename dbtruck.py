import subprocess, sys, csv, datetime, math, os, logging
from collections import *
from dateutil.parser import parse as dateparse

moduledir = os.path.abspath(os.path.dirname(__file__)) 
sys.path.extend( [moduledir, '%s/importers' % moduledir] )
from infertypes import *
from readers import *
from pg import PGMethods

logging.basicConfig()
_log = logging.getLogger(__file__)



def wc(fname):
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE)
    l = p.stdout.readline().strip()
    return int(l.split()[0])


def infer_header_row(rowiter, types):
    header = rowiter.next()
    htypes = map(get_type, header)
    matches = sum([ht == t and ht != None and t != str for ht, t in zip(htypes, types)])
    return matches == 0


def setup(fname, new, dbmethods):
    iterf = get_iter_f_csv(fname)
    types = infer_col_types(iterf())
    nlines = wc(fname)
    rowiter = iterf()

    header = None
    if infer_header_row(iterf(), types):
        header = rowiter.next()

    _log.info( 'types:\t%s', ' '.join(map(str,types)) )
    _log.info( 'headers:\t%s', header and ' '.join(header) or 'no header found' )

    dbmethods.setup_table(types, header, new)
    return rowiter, types


def transform_and_validate(types, row):
    row = map(str, map(str2sqlval, zip(types, row)))
    val = map(validate_type, zip(types, row))
    if reduce(lambda a,b: a and b, val):
        return row
    return None
    

def import_iterator(rowiter, types, dbmethods):
    blocksize = 10000
    buf = []

    for rowidx, row in enumerate(rowiter):
        row = transform_and_validate(types, row)
        if row is not None:
            buf.append(row)
        else:
            print >>dbmethods.errfile, ','.join(row)

        if len(buf) > 0 and len(buf) % blocksize == 0:
            success = dbmethods.import_block(buf)
            _log.info( "loaded\t%s\t%d", success, rowidx )
            buf = []

    if len(buf) > 0:
        success = dbmethods.import_block(buf)
    _log.info( "loaded\t%s\t%d", success, rowidx )




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
#pg_copy(sys.argv[1], sys.argv[2], 'intel')


