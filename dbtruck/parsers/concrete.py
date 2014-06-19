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
from openpyxl import load_workbook
from StringIO import StringIO

from dataiter import DataIterator
from dbtruck.util import get_logger, to_utf
from dbtruck.parsers.util import *


_log = get_logger()

class Parser(object):
    def __init__(self, f, fname, **kwargs):
        self.f = f
        self.fname = fname

    def get_data_iter(self):
        raise "Not implemented"


class CSVFileParser(Parser):
    def __init__(self, f, fname, **kwargs):
        Parser.__init__(self, f, fname, **kwargs)
        self.dialect = None

    def get_data_iter(self):
        # CSV/delimiter based lines
        # return a function that returns iter
        self.f.seek(0)
        if self.dialect is None:
          self.dialect = dialect = csv.Sniffer().sniff(self.f.read(15000))
          self.f.seek(0)

        if not self.dialect:
          raise "Could not parse using CSV"

        def _reader():
          self.f.seek(0)
          return csv.reader(self.f, dialect)
        return DataIterator(_reader, fname=self.fname)


        # Deprecated, just use csv.Sniffer instead of this

        # DELIMITERS = [' ', ',', ';', '\t', '-', ' - ', '=', ' = ']
        # bestdelim, bestperc, bestncols = None, 0., 0
        # for delim in DELIMITERS:
        #     perc_maj, ncols =  rows_consistent(_get_reader(self.f, delim))
        #     _log.debug("csvparser\t'%s'\tperc(%.3f)\tncols(%d)", delim, perc_maj, ncols )
        #     if ((ncols <= bestncols and perc_maj > 1.5 * bestperc) or
        #         (ncols > bestncols and bestperc - perc_maj < 0.1)):                
        #         bestdelim, bestperc, bestncols = delim, perc_maj, ncols
        # if bestncols:
        #     _log.debug("csvparser\tbest delim\t'%s'\tncols(%d)", bestdelim, bestncols )
        #     return DataIterator(lambda: _get_reader(self.f, bestdelim), fname=self.fname)
        # raise "Could not parse using CSV"



class OffsetFileParser(Parser):
    def __init__(self, f, fname, **kwargs):
        if 'offset' not in kwargs:
            s = ''#raw_input('can you give me the offsets to split each row? (otherwise, just press <enter>)\n> ').strip()
            offsets = self.parse_offset_str(s)
            if not offsets:
                raise RuntimeError
        else:
            offsets = kwargs['offset']
            
        self.offsets = self.normalize_offsets(offsets)
        super(OffsetFileParser, self).__init__(f, fname, **kwargs)

    def parse_offset_str(self, s):
        """
        look for groups of numbers (offsets) delimited by non a-z characters
        """
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
            for line in self.f:
                arr = [line[s:e] if s < len(line) else '' for s,e in offpairs]
                yield arr
        return DataIterator(_f, fname=self.fname)

class InferOffsetFileParser(OffsetFileParser):
  def __init__(self, f, fname, **kwargs):
    Parser.__init__(self, f, fname, **kwargs)

    self.offsets = self.infer_offsets()
    if not self.offsets:
      raise RuntimeError

    _log.info("inferoffset got offsets: %s", self.offsets)

  def remove_nonascii(self, l):
    l = re.sub("^[^\x00-\x7F]+", "", l)
    l = re.sub("[^\x00-\x7F]+$", "", l)
    return l


  def infer_offsets(self):
    f = self.f
    f.seek(0)
    # remove bad characetrs from the "header" row
    l = self.remove_nonascii(f.readline())
    get_start_idxs = lambda l: [i.start() for i in re.finditer("[^\s]+", l)]

    startIdxs = get_start_idxs(l)
    candidates = Counter(startIdxs)
    _log.info("inferoffset candidates: %s", sorted(candidates.keys()))

    # in the first 100 rows, are any of the candidates consistently matched?
    nlines = 0
    for l in f:
      candidates.update(filter(candidates.has_key, get_start_idxs(l)))
      if nlines >= 100:
        break
      nlines += 1

    _log.info("inferoffset nlines: %s", nlines)
    _log.info("inferoffset counts: %s", sorted(candidates.items()))

    # if there are a set of indexs that are > 99%-1 matched, then they are good offsets
    offsets = []
    for idx, count in candidates.iteritems():
      if count >= math.ceil(nlines * .99) - 1:
        offsets.append(idx)


    if len(offsets) > 2:
      return sorted(offsets)
    return None


  def get_data_iter(self):
    offpairs = zip(self.offsets[:-1], self.offsets[1:])
    def _f():
      self.f.seek(0)
      for idx, line in enumerate(self.f):
        if idx == 0:
          line = self.remove_nonascii(line)
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


class OldExcelParser(Parser):
    def __init__(self, f, fname, sheet=None, **kwargs):
        # ignore f
        self.fname = fname
        self.s = sheet
        if not sheet:
            raise "ExcelParser expects a sheet"
    
    def get_data_iter(self):
        # if first row with data has style and non of the other rows have style
        # then it's a header row
        sheet = self.s
        nrows = sheet.nrows
        rows = [sheet.row(i) for i in xrange(nrows)]
        if len(rows) <= 1:
            return None


        # skip empty rows
        # empty rows typically have much fewer non-empty columns
        # than the rest of the rows
        idx = 0
        while idx < len(rows):
            ncontents = len([c for c in rows[idx] if c.ctype != 0])
            if ncontents > 0.3 * nrows or ncontents > max(1, nrows - 2):
                break
            idx += 1
        rows = rows[idx:]
        
        # header = None        
        # if sum(1 for c in rows[0] if c.) > 0.8 * len(rows[0]):
        #     header = [c.value for c in rows[0]]
        #     rows = rows[1:]
        
        rows = [[to_utf(c.value) for c in r] for r in rows]            

        return DataIterator(lambda: iter(rows), fname=self.fname)



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
    try:
      from pyquery import PyQuery
    except:
      print >>sys.stderr, "could not import pyquery"
      return None

    def get_data_iter(self):
        self.f.seek(0)
        table = PyQuery(self.f.read())

        header = None
        rows = []
        counter = Counter()
        headers = []
        bheaders = True
        trs = table('tr')        
        for tr_el in trs:
            tr = PyQuery(tr_el)

            if bheaders:
                ths = tr('th')
                if ths:
                    headers.append([PyQuery(th).text() for th in ths])

            tds = tr('td')

            if tds:
                row = [PyQuery(td).text() for td in tds]
                rows.append(row)
                counter[len(row)] += 1
                bheaders = False

        if len(counter):
            ncols = counter.most_common(1)[0][0]
            headers = filter(lambda h: len(h) == ncols, headers)
            if headers:
                header = headers[-1]
        return DataIterator(lambda: iter(rows), header=header, fname=self.fname)

