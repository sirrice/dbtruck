import os
import sys
import csv
import logging
import datetime
import math
import re
import json
import codecs
import zipfile
import itertools

from operator import and_
from collections import *
from dateutil.parser import parse as dateparse
from pyquery import PyQuery as pq

from dataiter import DataIterator
from ..util import get_logger
from util import *
from dbtruck.parsers.util import _get_reader


_log = get_logger()
ZIPDIRNAME = '__dbtruck__unzips__'


class Parser(object):
    def __init__(self, f, **kwargs):
        self.f = f

    def get_data_iter(self):
        raise "Not implemented"


class CSVFileParser(Parser):
    def __init__(self, f, **kwargs):
        super(CSVFileParser, self).__init__(f, **kwargs)

    def get_data_iter(self):
        # CSV/delimiter based lines
        # return a function that returns iter
        DELIMITERS = [None, ',', ';', '\t', ' ']
        bestdelim, bestncols = None, 0
        for delim in DELIMITERS:
            consistent, ncols =  rows_consistent(_get_reader(self.f, delim))
            _log.debug("csvparser\t'%s'\tconsistent(%s)\tncols(%d)", delim, consistent, ncols )
            if consistent:
                if ncols > bestncols:
                    bestdelim, bestncols = delim, ncols
        if bestncols:
            _log.debug("csvparser\tbest delim\t'%s'\tncols(%d)", bestdelim, bestncols )
            return DataIterator(lambda: _get_reader(self.f, bestdelim))
        raise "Could not parse using CSV"

class OffsetFileParser(Parser):
    def __init__(self, f, **kwargs):
        if 'offset' not in kwargs:
            s = raw_input('can you give me the offsets to split each row?\n> ').strip()
            offsets = self.parse_offset_str(s)
            if not offsets:
                raise RuntimeError
        else:
            offsets = kwargs['offset']
            
        self.offsets = self.normalize_offsets(offsets)
        super(OffsetFileParser, self).__init__(f, **kwargs)

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
        _log.debug( "offsetparser\tnormalized offsets: %s", str(arr) )
        return arr
        
    
    def get_data_iter(self):
        offpairs = zip(self.offsets[:-1], self.offsets[1:])
        def _f():
            self.f.seek(0)
            for line in f:
                line = line.strip()
                arr = [line[s:e] if s < len(line) else '' for s,e in offpairs]
                yield arr
        return DataIterator(_f)

class SingleColumnParser(Parser):
    def __init__(self, f, **kwargs):
        super(SingleColumnParser, self).__init__(f, **kwargs)

    def get_data_iter(self):
        def _f():
            self.f.seek(0)
            for line in self.f:
                yield [line.strip()]
        return DataIterator(_f)


class JSONParser(Parser):

    def list_is_consistent(self, l):
        if len(l) == 0:
            return True

        mastertype = type(l[0])
        for v in l:
            if type(v) != mastertype:
                return False
        return True

    def obj_to_row(self, o):
        if isinstance(o, str):
            return [o]
        if isinstance(o, dict):
            return o.values()
        if isinstance(o, list):
            return o
        return [o]

    def list_of_dict_iterator(self, l):
        keys = set()
        for d in l:
            keys.update(d.keys())
        keys = list(keys)

        def _f():
            for d in l:
                yield [d.get(key, '')  for key in keys]
        return DataIterator(_f, header=keys)

    def get_data_iter(self):
        """This methad assumes that JSON file is either
        1) a list of dictionaries
        2) a dictionary with an entry that contains a list of dictionaries
           and finds the longest list
        
        """
        
        self.f.seek(0)
        dec = json.JSONDecoder('utf-8', strict=False)
        obj, extra = dec.raw_decode(self.f.read())
        _log.debug("jsonparser.iterator\textra data: %s", extra)

        # first see if the object is a dictionary, and look for the key
        # containing the longest list
        bestlist = None
        if isinstance(obj, dict):
            bestlist, bestlen = None, 0
            for key, val in obj.iteritems():
                if (isinstance(val, list) and
                    len(val) > bestlen and
                    self.list_is_consistent(val)):
                    bestlist, bestlen  = val, len(val)
        elif isinstance(obj, list):
            bestlist = obj
        
        if not bestlist:
            return None

        if isinstance(bestlist[0], dict):
            return self.list_of_dict_iterator(bestlist)
        elif isinstance(best_list[0], list):
            return DataIterator(lambda: bestlist)
        raise

class ExcelParser(Parser):
    pass

class HTMLTableParser(Parser):
    """
    Takes the html of a <table> element and constructs an iterator
    """
    def iterator(self):
        raise









def get_readers(fname, **kwargs):
    """
    @return a list of functions that return row iterators.  This is so files such as HTML
    can return multiple tables to be stored
    """
    # TODO: try testing other file formats
    # json, html etc
    _log.info("processing\t%s", fname)
    if os.path.isdir(fname):
        dataiters = []
        args = {'kwargs' : kwargs, 'dataiters' : dataiters}
        os.path.walk(fname, get_readers_walk_cb, args)
        return dataiters
    elif zipfile.is_zipfile(fname):
        with zipfile.ZipFile(fname, 'r') as zp:
            if not os.path.exists(ZIPDIRNAME):
                os.mkdir(ZIPDIRNAME)
            prefix = os.path.join(ZIPDIRNAME, fname)
            
            _log.info("unzipping datafiles from %s in", fname)
            namelist = zp.namelist()
            namelist = filter(lambda name: (not name.startswith('/')
                                            and not('..' in name)),
                                            namelist)
            zp.extractall(prefix, namelist)

            
            namelist = map(lambda name: os.path.join(prefix, name), namelist)
            return itertools.chain(*map(get_readers, namelist))
    return [get_reader_from_text_file(fname, **kwargs)]

def get_readers_walk_cb(args, dirname, fnames):
    kwargs = args['kwargs']
    dataiters = args['dataiters']
    for fname in fnames:
        fullname = os.path.join(dirname, fname)
        if not os.path.isdir(fullname):
            for dataiter in get_readers(fullname, **kwargs):
                dataiter.fname = fname
                dataiter.file_index = len(dataiters)
                dataiters.append(dataiter)
    

text_parsers = [CSVFileParser, JSONParser]
def get_reader_from_text_file(fname, **kwargs):
    bestiter, bestparser, bestncols = None, None, 1
    for parser in text_parsers:
        try:
            with file(fname, 'r') as f:
                p = parser(f, **kwargs)
                i = p.get_data_iter()
                consistent, ncols = rows_consistent(i())
                if consistent and ncols > bestncols:
                    bestiter, bestparser, bestncols = i, parser, ncols
        except KeyboardInterrupt:
            pass
        except Exception as e:
            _log.info(e)
    if not bestiter:
        _log.debug("Checking to see if user has offsets")
        with file(fname, 'r') as f:
            p = parser(f, **kwargs)
            i = p.get_data_iter()
            consistent, ncols = rows_consistent(i())
            if consistent and ncols > bestncols:
                bestiter, bestparser, bestncols = i, parser, ncols

    if not bestiter:
        _log.debug("Could not parse file. Defaulting to single column format")        
        bestparser = SingleColumnParser
        
    _log.debug("text file parser: %s\t%s", bestparser.__name__, fname)
    f = file(fname, 'r')
    dataiter = bestparser(f, **kwargs).get_data_iter()
    dataiter.fname = fname
    return dataiter



if __name__ == '__main__':

    fname = '/Users/sirrice/Desktop/lastpermissiondenied.json'
    readers = get_readers(fname)

    for reader in readers:
        i = 0        
        for r in reader():
            i += 1
        print i
