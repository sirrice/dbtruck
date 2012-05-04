import sys, csv, logging, datetime, math, re
from operator import and_
from collections import *
from dateutil.parser import parse as dateparse
from dbtruck.parsers.util import *
from dbtruck.parsers.util import _get_reader


logging.basicConfig()
_log = logging.getLogger(__file__)


class DataFileDelimiter(object):
    """
    Tries a bunch of different tricks for a given file type
    """

    def __init__(self, f, **kwargs):
        self.f = f

    def iterator_func(self):
        raise RuntimeError
        return []

class CSVFileDelimiter(DataFileDelimiter):
    def __init__(self, f, **kwargs):
        super(CSVFileDelimiter, self).__init__(f, **kwargs)

    def iterator_func(self):
        # CSV/delimiter based lines
        # return a function that returns iter
        DELIMITERS = [None, ',', ';', '\t', ' ']
        bestdelim, bestncols = None, 0
        for delim in DELIMITERS:
            consistent, ncols =  rows_consistent(_get_reader(self.f, delim))
            _log.debug( 'delimiters\t%s\t%st%d', delim, consistent, ncols )
            if consistent:
                if ncols > bestncols:
                    bestdelim, bestncols = delim, ncols
        if bestncols:
            _log.debug( "best delimitor\t%s\t%d", bestdelim, bestncols )
            return lambda: _get_reader(self.f, bestdelim)
        raise "Could not parse using CSV"

class OffsetFileDelimiter(DataFileDelimiter):
    def __init__(self, f, **kwargs):
        if 'offset' not in kwargs:
            s = raw_input('can you give me the offsets to split each row?\n> ').strip()
            offsets = self.parse_offset_str(s)
            if not offsets:
                raise RuntimeError
        else:
            offsets = kwargs['offset']
            
        self.offsets = self.normalize_offsets(offsets)
        super(OffsetFileDelimiter, self).__init__(f, **kwargs)

    def parse_offset_str(self, s):
        if not s:
            return None
        if re.search('[a-zA-Z]', s):
            return None
        delimiter = re.search('\D+', s).group()
        if not delimiter:
            return None
        arr = map(int, s.split(delimiter))
        
        return arr

    def normalize_offsets(self, arr):
        # if arr is strictly increasing, then they are absolute offsets
        # otherwise they are relative
        increasing = reduce(and_, ( arr[i+1]>arr[i] for i in xrange(len(arr)-1)) )
        if not increasing:
            arr = map( lambda i: sum(arr[:i]), range(len(arr)) )
        _log.debug( "normalized offsets: %s", str(arr) )
        return arr
        
    
    def iterator_func(self):
        offpairs = zip(self.offsets[:-1], self.offsets[1:])
        def _f():
            self.f.seek(0)
            for line in f:
                line = line.strip()
                arr = [line[s:e] if s < len(line) else '' for s,e in offpairs]
                yield arr
        return _f

class SingleColumnDelimiter(DataFileDelimiter):
    def __init__(self, f, **kwargs):
        super(SingleColumnDelimiter, self).__init__(f, **kwargs)

    def iterator_func(self):
        def _f():
            self.f.seek(0)
            for line in f:
                yield [line.strip()]
        return _f
