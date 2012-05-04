import sys
import csv
import logging
import datetime
import math
import re
import json
import codecs

from operator import and_
from collections import *
from dateutil.parser import parse as dateparse
from pyquery import PyQuery as pq

from delimiters import *
from util import *
from dbtruck.parsers.util import _get_reader

logging.basicConfig()
_log = logging.getLogger(__file__)



class Parser(object):
    def __init__(self, f, **kwargs):
        self.f = f

    def iterator_func(self):
        raise "Not implemented"
        pass

class TextParser(Parser):

    def iterator(self):
        self.f.seek(0)
        for line in self.f:
            yield line

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
        yield keys
        for d in l:
            yield [d.get(key, '')  for key in keys]

    def iterator_func(self):
        return lambda: self.iterator()
    
    def iterator(self):
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
            return

        if isinstance(bestlist[0], dict):
            return self.list_of_dict_iterator(bestlist)
        elif isinstance(best_list[0], list):
            return bestlist
        raise

class ExcelParser(Parser):
    pass

class HTMLTableParser(Parser):
    """
    Takes the html of a <table> element and constructs an iterator
    """
    def iterator(self):
        raise









def get_reader(fname, **kwargs):
    # TODO: try testing other file formats
    # json, html etc
    
    return get_reader_from_text_file(fname, **kwargs)


text_parsers = [CSVFileDelimiter, JSONParser, OffsetFileDelimiter]
def get_reader_from_text_file(fname, **kwargs):
    bestiter, bestparser, bestncols = None, None, 0
    for parser in text_parsers:
        try:
            with file(fname, 'r') as f:
                p = parser(f, **kwargs)
                i = p.iterator_func()
                consistent, ncols = rows_consistent(i())
                if consistent and ncols > bestncols:
                    bestiter, bestparser, bestncols = i, parser, ncols
        except KeyboardInterrupt:
            pass
        except Exception as e:
            import traceback
            traceback.print_exc()
    if bestiter:
        _log.debug("best parser: %s", parser.__name__)
        f = file(fname, 'r')
        return bestparser(f, **kwargs).iterator_func()

    _log.debug("Could not parse file, defaulting to single column format")
    p = SingleColumnDelimiter(fname, **kwargs)
    return p.iterator_func()


if __name__ == '__main__':

    fname = '/Users/sirrice/Desktop/lastpermissiondenied.json'
    reader = get_reader(fname)
    i = 0
    for r in reader():
        i += 1
    print i
