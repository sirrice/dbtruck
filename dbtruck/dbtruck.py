import subprocess
import sys
import csv
import datetime
import math
import os
import logging
import re
import time

from collections import *
from dateutil.parser import parse as dateparse



moduledir = os.path.abspath(os.path.dirname(__file__)) 
sys.path.append( moduledir )
from infertypes import *
from parsers.parsers import *


logging.basicConfig()
_log = logging.getLogger(__file__)
re_space = re.compile('\s+')
re_nonascii = re.compile('[^\w\s]')
re_nonasciistart = re.compile('^[^\w]')


def wc(fname):
    p = subprocess.Popen(['wc', '-l', fname], stdout=subprocess.PIPE)
    l = p.stdout.readline().strip()
    return int(l.split()[0])


def infer_header_row(rowiter, types):
    header = rowiter.next()
    htypes = map(get_type, header)
    matches = sum([ht == t and ht != None and t != str for ht, t in zip(htypes, types)])
    return matches == 0

def clean_header(value):
    return re_nonasciistart.sub('', re_space.sub('_', re_nonascii.sub('', value.strip()).strip()))



def import_datafiles(fname, new, tablename, dbname, errfile, exportmethodsklass):
    iterf_list = get_readers(fname)
    for idx, iterf in enumerate(iterf_list):
        try:
            if len(iterf_list) > 1:
                newtablename = '%s_%d' % (tablename, idx)            
            else:
                newtablename = tablename
            exportmethods = exportmethodsklass(newtablename, dbname, errfile)

            infer_metadata(iterf)
            exportmethods.setup_table(iterf.types, iterf.header, new)
            import_iterator(iterf(), iterf.types, exportmethods)
        except:
            import traceback
            traceback.print_exc()


def infer_metadata(iterf):
    if not iterf.types:
        iterf.types = types = infer_col_types(iterf())

    if not iterf.header:
        rowiter = iterf()
        header = None
        if infer_header_row(iterf(), types):
            header = rowiter.next()
            header = map(clean_header, header)
            iterf.header = header
    _log.info( 'types:\t%s', ' '.join(map(str, iterf.types)) )
    _log.info( 'headers:\t%s', iterf.header and ' '.join(iterf.header) or 'no header found' )



def transform_and_validate(types, row):
    row = map(str2sqlval, zip(types, row))
    return row
    #val = map(validate_type, zip(types, row))
    if reduce(lambda a,b: a and b, val):
        return row
    return None
    

def import_iterator(rowiter, types, dbmethods):
    blocksize = 100000
    buf = []

    start = time.time()
    for rowidx, row in enumerate(rowiter):
        row = transform_and_validate(types, row)
        if row is not None:
            buf.append(row)
        else:
            print >>dbmethods.errfile, ','.join(row)

        if len(buf) > 0 and len(buf) % blocksize == 0:
            print "transform_val\t", (time.time() - start)
            success = dbmethods.import_block(buf)
            _log.info( "loaded\t%s\t%d", success, rowidx )
            buf = []
            start = time.time()            

    if len(buf) > 0:
        success = dbmethods.import_block(buf)
    _log.info( "loaded\t%s\t%d", success, rowidx )


