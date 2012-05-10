import subprocess
import sys
import csv
import datetime
import math
import os
import logging
import re
import time
import pdb

from collections import *
from dateutil.parser import parse as dateparse

moduledir = os.path.abspath(os.path.dirname(__file__)) 
sys.path.append( moduledir )
from infertypes import *
from parsers.parsers import *
from util import get_logger, to_utf


_log = get_logger()
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

    if matches > 0:
        return False

    if max(map(len, header)) > 100:
        return False

    # lots of more complex analysis goes HERE

    return True


def clean_header(value):
    ret = re_nonasciistart.sub('', re_space.sub('_', re_nonascii.sub('', value.strip()).strip()))
    ret = str(unicode(ret, errors='ignore'))
    return ret.lower()

def infer_metadata(iterf):
    if not iterf.types:
        iterf.types = infer_col_types(iterf)

    if not iterf.header:
        rowiter = iterf()
        header = None
        if infer_header_row(iterf(), iterf.types):
            header = rowiter.next()
            iterf.header = header
        else:
            iterf.header = ['attr%d' % i for i in xrange(len(iterf.types))]

    iterf.header = map(clean_header, iterf.header)

            
    _log.info( 'types:\t%s', ' '.join(map(str, iterf.types)) )
    _log.info( 'headers:\t%s', iterf.header and ' '.join(iterf.header) or 'no header found' )


def get_readers_from_list(fnames):
    for fname in fnames:
        for reader in get_readers(fname):
            yield reader

def import_datafiles(fnames, new, tablename, dbname, errfile, exportmethodsklass):
    for idx, iterf in enumerate(get_readers_from_list(fnames)):
        try:
            if idx > 0:
                newtablename = '%s_%d' % (tablename, idx)            
            else:
                newtablename = tablename

            exportmethods = exportmethodsklass(newtablename, dbname, errfile)

            infer_metadata(iterf)
            exportmethods.setup_table(iterf.types, iterf.header, new)
            import_iterator(iterf, exportmethods)
        except:
            import traceback
            traceback.print_exc()



def transform_and_validate(types, row):
    #row = map(str2sqlval, zip(types, row))
    return row
    #val = map(validate_type, zip(types, row))
    if reduce(lambda a,b: a and b, val):
        return row
    return None
    

def import_iterator(iterf, dbmethods):
    """
    """
    # this function could dynamically increase or decrease the block
    rowiter = iterf()
    types = iterf.types
    blocksize = 100000
    buf = []

    for rowidx, row in enumerate(rowiter):
        row = transform_and_validate(types, row)

        if row is not None and len(row) == len(iterf.types):
            buf.append(row)
        elif row is not None and len(row) != len(iterf.types):
            print >>dbmethods.errfile, ','.join(map(to_utf, row))

        if len(buf) > 0 and len(buf) % blocksize == 0:
            success = dbmethods.import_block(buf, iterf)
            buf = []

    if len(buf) > 0:
        success = dbmethods.import_block(buf, iterf)
    _log.info( "loaded\t%s\t%d", success, rowidx )


