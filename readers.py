import sys, csv, logging, datetime
from collections import *
from dateutil.parser import parse as dateparse
import math

logging.basicConfig()
_log = logging.getLogger(__file__)

def rows_consistent(iter):
    """
    iterates through first 10k rows in iterator and records the number of
    columns in each row.  Checks that >98% of the rows have the same number of columns
    @return (isconsistent, number of columns)
    """
    iter.next()
    lens = Counter()
    for idx, row in enumerate(iter):
        lens[len(row)] += 1
        if idx > 10000: break
    maj = lens.most_common(1)[0][1]
    tot = sum(lens.values())
    return float(maj)/tot >= 0.98, lens.most_common(1)[0][0]
    

def _get_reader(fname, delim):
    """
    creates an CSV based iterator
    """
    f = (line.strip() for line in open(fname, 'r'))
    if delim is None:
        return csv.reader(f)
    return csv.reader(f, delimiter=delim)

def get_reader(fname, offset=None):
    """
    try a bunch of possibilities
    """
    if offset is None:
        return get_iter_f_csv(fname)
    return get_iter_f_offset(fname, offset)

# CSV/delimiter based lines
# return a function that returns iter
def get_iter_f_csv(fname):
    DELIMITERS = [None, ',', ';', '\t', ' ']
    bestdelim, bestncols = None, 0
    for delim in DELIMITERS:
        consistent, ncols =  rows_consistent(_get_reader(fname, delim))
        _log.debug( 'delimiters\t%s\t%st%d', delim, consistent, ncols )
        if consistent:
            if ncols > bestncols:
                bestdelim, bestncols = delim, ncols
    if bestncols:
        _log.debug( "best delimitor\t%s\t%d", bestdelim, bestncols )
        return lambda: _get_reader(fname, bestdelim)
    raise "Arg, can't parse file, try constant offset instead?"

# strict offset based lines
def get_iter_f_offset(fname):
    raise "Not implemented"
