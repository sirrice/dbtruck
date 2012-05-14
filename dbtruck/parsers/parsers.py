import os
import sys
import csv
import pdb
import logging
import datetime
import traceback
import requests
import math
import re
import json
import codecs
import zipfile
import itertools

from operator import and_
from collections import *
from dateutil.parser import parse as dateparse
from pyquery import PyQuery
from openpyxl import load_workbook
from StringIO import StringIO

from dataiter import DataIterator
from ..util import get_logger
from util import *
from dbtruck.parsers.util import _get_reader
from ..settings import ZIPDIRNAME, DLDIRNAME


_log = get_logger()


class Parser(object):
    def __init__(self, f, fname, **kwargs):
        self.f = f
        self.fname = fname

    def get_data_iter(self):
        raise "Not implemented"


class CSVFileParser(Parser):
    def get_data_iter(self):
        # CSV/delimiter based lines
        # return a function that returns iter

        DELIMITERS = [' ', ',', ';', '\t', '-', ' - ', '=', ' = ']
        bestdelim, bestperc, bestncols = None, 0., 0
        for delim in DELIMITERS:
            perc_maj, ncols =  rows_consistent(_get_reader(self.f, delim))
            _log.debug("csvparser\t'%s'\tperc(%.3f)\tncols(%d)", delim, perc_maj, ncols )
            if ((ncols <= bestncols and perc_maj > 1.5 * bestperc) or
                (ncols > bestncols and bestperc - perc_maj < 0.1)):                
                bestdelim, bestperc, bestncols = delim, perc_maj, ncols
        if bestncols:
            _log.debug("csvparser\tbest delim\t'%s'\tncols(%d)", bestdelim, bestncols )
            return DataIterator(lambda: _get_reader(self.f, bestdelim), fname=self.fname)
        raise "Could not parse using CSV"

class OffsetFileParser(Parser):
    def __init__(self, f, fname, **kwargs):
        if 'offset' not in kwargs:
            s = raw_input('can you give me the offsets to split each row? (otherwise, just press <enter>)\n> ').strip()
            offsets = self.parse_offset_str(s)
            if not offsets:
                raise RuntimeError
        else:
            offsets = kwargs['offset']
            
        self.offsets = self.normalize_offsets(offsets)
        super(OffsetFileParser, self).__init__(f, fname, **kwargs)

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
        return DataIterator(_f, fname=self.fname)

class SingleColumnParser(Parser):
    def get_data_iter(self):
        def _f():
            self.f.seek(0)
            for line in self.f:
                yield [line.strip()]
        return DataIterator(_f, fname=self.fname)


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
        return DataIterator(_f, header=keys, fname=self.fname)

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
            return DataIterator(lambda: bestlist, fname=self.fname)
        raise

class ExcelParser(Parser):
    def __init__(self, f, fname, sheet=None, **kwargs):
        # ignore f
        self.fname = fname
        self.s = sheet
        if not sheet:
            raise "ExcelParser expects a sheet"
    
    def get_data_iter(self):
        # if first row with data has style and non of the other rows have style
        # then it's a header row
        rows = self.s.rows
        if len(rows) <= 1:
            return None


        # skip empty rows
        idx = 0
        while idx < len(rows):
            if [c for c in rows[idx] if str(c.value).strip()]:
                break
            idx += 1
        rows = rows[idx:]
        
        header = None        
        if sum(1 for c in rows[0] if c.has_style) > 0.8 * len(rows[0]):
            header = [c.value for c in rows[0]]
            rows = rows[1:]
        
        rows = [[c.value for c in r] for r in rows]            

        return DataIterator(lambda: iter(rows), header=header, fname=self.fname)

class HTMLTableParser(Parser):
    """
    Takes a <table> element and constructs an 

    looks for tables that are of the form
    table
     [tr
       th*]
     tr*
       td*

    the text within each td and th is used as the content of the table
    assumes that th is always and only used as the table's header
    """
    def get_data_iter(self):
        self.f.seek(0)
        table = PyQuery(self.f.read())
        ths = table('th')
        header = [PyQuery(th).text() for th in ths] if ths else None

        trs = table('tr')
        rows = []
        for tr_el in trs:
            tr = PyQuery(tr_el)
            tds = tr('td')
            if tds:
                row = [PyQuery(td).text() for td in tds]
                rows.append(row)
        return DataIterator(lambda: iter(rows), header=header, fname=self.fname)



def get_readers(fname, **kwargs):
    """
    @return a list of functions that return row iterators.  This is so files such as HTML
    can return multiple tables to be stored
    """
    # XXX: skip binary files that are not supported
    #      http://www.garykessler.net/library/file_sigs.html
    _log.info("processing\t%s", fname)

    if is_url(fname):
        return get_readers_from_url(fname)
    elif is_url_file(fname):
        return get_readers_from_url_file(fname, **kwargs)
    elif is_excel_file(fname):
        return get_readers_from_excel_file(fname, **kwargs)
    elif os.path.isdir(fname):
        dataiters = []
        args = {'kwargs' : kwargs, 'dataiters' : dataiters}
        os.path.walk(fname, get_readers_walk_cb, args)
        return dataiters
    elif zipfile.is_zipfile(fname):
        return get_readers_from_zip_file(fname, **kwargs)
    elif is_html_file(fname):
        return get_readers_from_html_file(fname, **kwargs)
    return get_readers_from_text_file(fname, **kwargs)

def get_readers_walk_cb(args, dirname, fnames):
    """
    talkback for directory walk
    """
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
def get_readers_from_text_file(fname, **kwargs):
    bestparser, bestperc, bestncols = None, 0., 1
    for parser in text_parsers:
        try:
            with file(fname, 'r') as f:
                p = parser(f, fname, **kwargs)
                i = p.get_data_iter()
                perc_maj, ncols = rows_consistent(i())
                if perc_maj > bestperc and perc_maj > 0.6 and ncols > bestncols:
                    bestparser, bestperc, bestncols = parser, perc_maj, ncols
        except KeyboardInterrupt:
            pass
        except Exception as e:
            _log.info(e)

    if not bestparser:
        _log.debug("Checking to see if user has offsets")
        try:
            with file(fname, 'r') as f:
                p = OffsetFileParser(f, fname, **kwargs)
                i = p.get_data_iter()
                bestparser, bestncols = parser, ncols
        except:
            pass

    if not bestparser:
        _log.debug("Could not parse file. Defaulting to single column format")        
        bestparser = SingleColumnParser
        
    _log.debug("text file parser: %s\t%s", bestparser.__name__, fname)
    f = file(fname, 'r')
    dataiter = bestparser(f, fname, **kwargs).get_data_iter()
    dataiter.fname = fname
    return [dataiter]


def find_ideal_tables(tables):
    rm = []
    for table in tables:
        found = False        
        for t2 in tables:
            if table == t2:
                continue
            t2 = PyQuery(t2)
            _t = PyQuery(table)
            while len(_t):
                if _t == t2:
                    found = True
                    break
                _t = _t.parent()
        if found:
            rm.append(table)
    ret = [table for table in tables if table not in rm]
    return ret

def get_readers_from_html_content(fname, html, **kwargs):
    parsers = []
    pq = PyQuery(html)
    tables = find_ideal_tables(pq('table'))
    
    for table_el in tables:
        try:
            table = PyQuery(table_el)
            p = HTMLTableParser(StringIO(table.html()), fname, **kwargs)
            i = p.get_data_iter()
            consistent, ncols = html_rows_consistent(i())
            if consistent and ncols > 1:
                parsers.append(i)
        except KeyboardInterrupt:
            pass
        except Exception as e:
            _log.info(traceback.format_exc())
    return parsers

def get_readers_from_html_file(fname, **kwargs):
    try:
        with file(fname, 'r') as f:
            return get_readers_from_html_content(fname, f.read(), **kwargs)
    except:
        _log.info(traceback.format_exc())
        return []

def get_readers_from_url(url, **kwargs):
    try:
        _log.debug("fetching url %s", url)
        if not os.path.exists(DLDIRNAME):
            os.mkdir(DLDIRNAME)
        req = requests.get(url)
        fname = os.path.join(DLDIRNAME, url.replace('/', '_'))
        with file(fname, 'w') as f:
            f.write(req.content)
        return get_readers(fname, **kwargs)
    except:
        _log.info(traceback.format_exc())
        return []

def get_readers_from_url_file(fname, **kwargs):
    try:
        with file(fname, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                for reader in get_readers(line, **kwargs):
                    yield reader
    except:
        _log.info(traceback.format_exc())
        
def get_readers_from_zip_file(fname, **kwargs):
    try:
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
    except Exception as e:
        _log.info('get_from_zip_file\t%s', e)
        return []


def get_readers_from_excel_file(fname, **kwargs):
    ret = []
    try:
        w = load_workbook(fname)
        for sname in w.get_sheet_names():
            s = w.get_sheet_by_name(sname)
            try:
                parser = ExcelParser(None, fname, sheet=s)
                i = parser.get_data_iter()
                consistent, ncols = html_rows_consistent(i())
                if consistent and ncols > 1:
                    ret.append(i)
            except:
                _log.info(traceback.format_exc())
    except:
        _log.info(traceback.format_exc())
    return ret

    
if __name__ == '__main__':

    fname = '/Users/sirrice/Desktop/lastpermissiondenied.json'
    readers = get_readers(fname)

    for reader in readers:
        i = 0        
        for r in reader():
            i += 1
        print i
