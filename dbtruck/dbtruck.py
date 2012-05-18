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



def infer_header_row(rowiter, types):
    try:
        header = rowiter.next()
        htypes = map(get_type, header)
        matches = sum([ht == t and ht != None and t != str for ht, t in zip(htypes, types)])

        if matches > 0:
            return False

        if max(map(len, header)) > 100:
            return False

        # lots of more complex analysis goes HERE
        return True
    except:
        return False



def clean_header(header):
    newheader = []
    attridx = 0
    for value in header:
        try:
            ret = re_nonasciistart.sub('', re_space.sub('_', re_nonascii.sub('', value.strip()).strip()))
            ret = to_utf(ret).lower()
            if not ret:
                ret = 'attr%d' % attridx
            if re.match('\d+', ret):
                ret = '_%s' % ret
        except:
            print value
            ret = 'attr%d' % attridx
        attridx += 1
        newheader.append(ret.lower())

    # XXX: ensure that header doesn't have overlapping values
    if len(set(newheader)) < len(newheader):
        _log.info("duplicate elements in header\t%s", str(newheader))
        return None
    return newheader
    

def infer_metadata(iterf):
    """
    infers header and attribute type information
    augments iterator (iterf) with type and header arrays
    """
    if not iterf.types:
        iterf.types = infer_col_types(iterf)

    if not iterf.header and infer_header_row(iterf(), iterf.types):
        iterf.header = iterf().next()

    if iterf.header:
        iterf.header = clean_header(iterf.header)

    if not iterf.header:
        iterf.header = ['attr%d' % i for i in xrange(len(iterf.types))]

    if 'id' not in iterf.header:
        iterf.header.append('id')
        iterf.add_id_col = True
        iterf.types.append(int)
            
    _log.info( 'types:\t%s', ' '.join(map(str, iterf.types)) )
    _log.info( 'headers:\t%s', iterf.header and ' '.join(iterf.header) or 'no header found' )


def get_readers_from_list(fnames):
    for fname in fnames:
        for reader in get_readers(fname):
            yield reader

def import_datafiles(fnames, new, tablename, dbname, errfile, exportmethodsklass):
    try:
        new_errfile = False
        if not errfile:
            errfile = file('/tmp/dbtruck.errfile', 'a')
            new_errfile = True

        if isinstance(fnames, basestring):
            fnames = [fnames]

        idx = 0
        for iterf in get_readers_from_list(fnames):
            try:
                if idx > 0:
                    newtablename = '%s_%d' % (tablename, idx)            
                else:
                    newtablename = tablename

                infer_metadata(iterf)

                exportmethods = exportmethodsklass(newtablename, dbname, errfile)
                exportmethods.setup_table(iterf.types, iterf.header, new)
                import_iterator(iterf, exportmethods)
                idx += 1 # this is so failed tables can be reused
            except Exception as e:
                _log.warn(traceback.format_exc())

        if new_errfile:
            errfile.close()
    except:
        _log.warn(traceback.format_exc())


def transform_and_validate(types, row):
    row = map(str2sqlval, zip(types, row))
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

        if iterf.add_id_col:
            row.append(str(rowidx))

        if row is not None and len(row) == len(iterf.types):
            buf.append(row)
        elif row is not None and len(row) != len(iterf.types):
            print >>dbmethods.errfile, ','.join(map(to_utf, row))

        if len(buf) > 0 and len(buf) % blocksize == 0:
            try:
                success = dbmethods.import_block(buf, iterf)
            except Exception as e:
                _log.warn(traceback.format_exc())
            buf = []

    if len(buf) > 0:
        try:
            success = dbmethods.import_block(buf, iterf)
            _log.info( "loaded\t%s\t%d", success, rowidx )
        except Exception as e:
            _log.warn(traceback.format_exc())
        


