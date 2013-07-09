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

from concrete import *
from dataiter import DataIterator
from dbtruck.util import get_logger, to_utf
from dbtruck.parsers.util import *

_log = get_logger()


class DBTruckParser(object):
    def __init__(self, zipdirname, dldirname):
        self.zipdirname = zipdirname
        self.dldirname = dldirname

    def get_readers(self, fname, **kwargs):
        """
        @return a list of functions that return row iterators.  This is so files such as HTML
        can return multiple tables to be stored
        """
        # XXX: skip binary files that are not supported
        #      http://www.garykessler.net/library/file_sigs.html
        _log.info("processing\t%s", fname)

        if isinstance(fname, list) or isinstance(fname, set):
            return itertools.chain(*map(self.get_readers, fname))
        elif is_url(fname):
            return self.get_readers_from_url(fname)
        elif is_url_file(fname):
            return self.get_readers_from_url_file(fname, **kwargs)
        elif is_excel_file(fname):
            return self.get_readers_from_excel_file(fname, **kwargs)
        elif is_old_excel_file(fname):
            return self.get_readers_from_old_excel_file(fname, **kwargs)
        elif os.path.isdir(fname):
            dataiters = []
            args = {'kwargs' : kwargs, 'dataiters' : dataiters}
            os.path.walk(fname, self.get_readers_walk_cb, args)
            return dataiters
        elif zipfile.is_zipfile(fname):
            return self.get_readers_from_zip_file(fname, **kwargs)
        elif is_html_file(fname):
            return self.get_readers_from_html_file(fname, **kwargs)
        return self.get_readers_from_text_file(fname, **kwargs)

    def get_readers_walk_cb(self, args, dirname, fnames):
        """
        talkback for directory walk
        """
        kwargs = args['kwargs']
        dataiters = args['dataiters']
        for fname in fnames:
            fullname = os.path.join(dirname, fname)
            if not os.path.isdir(fullname):
                for dataiter in self.get_readers(fullname, **kwargs):
                    dataiter.fname = fname
                    dataiter.file_index = len(dataiters)
                    dataiters.append(dataiter)




                    
    def get_readers_from_text_file(self, fname, **kwargs):
        text_parsers = [CSVFileParser, JSONParser, InferOffsetFileParser]        
        bestparser, bestperc, bestncols = None, 0., 1
        for parser in text_parsers:
            try:
                _log.info("trying text reader:  %s", str(parser))
                with file(fname, 'rb') as f:
                    p = parser(f, fname, **kwargs)
                    i = p.get_data_iter()
                    perc_maj, ncols = rows_consistent(i())
                    if perc_maj > bestperc and perc_maj > 0.6 and ncols > bestncols:
                        bestparser, bestperc, bestncols = parser, perc_maj, ncols
                        _log.info("set best parser to %s", parser)
            except KeyboardInterrupt:
                pass
            except Exception as e:
                _log.info(e)

        if not bestparser:
            _log.debug("Checking to see if user has offsets")
            try:
                with file(fname, 'rb') as f:
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


    def find_ideal_tables(self, tables):
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

    def get_readers_from_html_content(self, fname, html, **kwargs):
        parsers = []
        pq = PyQuery(html)
        tables = self.find_ideal_tables(pq('table'))

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

    def get_readers_from_html_file(self, fname, **kwargs):
        try:
            with file(fname, 'rb') as f:
                return self.get_readers_from_html_content(fname, f.read(), **kwargs)
        except:
            _log.info(traceback.format_exc())
            return []

    def get_readers_from_url(self, url, **kwargs):
        try:
            _log.debug("fetching url %s", url)
            if not os.path.exists(self.dldirname):
                os.mkdir(self.dldirname)
            req = requests.get(url)
            fname = os.path.join(self.dldirname, url.replace('/', '_'))
            with file(fname, 'w') as f:
                f.write(req.content)
            return self.get_readers(fname, **kwargs)
        except:
            _log.info(traceback.format_exc())
            return []

    def get_readers_from_url_file(self, fname, **kwargs):
        try:
            with file(fname, 'rb') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    for reader in self.get_readers(line, **kwargs):
                        yield reader
        except:
            _log.info(traceback.format_exc())

    def get_readers_from_zip_file(self, fname, **kwargs):
        try:
            with zipfile.ZipFile(fname, 'r') as zp:
                if not os.path.exists(self.zipdirname):
                    os.mkdir(self.zipdirname)
                prefix = os.path.join(self.zipdirname, fname)

                _log.info("unzipping datafiles from %s in", fname)
                namelist = zp.namelist()
                namelist = filter(lambda name: (not name.startswith('/')
                                                and not('..' in name)),
                                                namelist)
                zp.extractall(prefix, namelist)


                namelist = map(lambda name: os.path.join(prefix, name), namelist)
                return itertools.chain(*map(self.get_readers, namelist))
        except Exception as e:
            _log.info('get_from_zip_file\t%s', e)
            return []


    def get_readers_from_excel_file(self, fname, **kwargs):
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

    def get_readers_from_old_excel_file(self, fname, **kwargs):
        ret = []
        try:
            w = xlrd.open_workbook(fname)
            for sheet in w.sheets():
                nrows, ncols = sheet.nrows, sheet.ncols
                try:
                    parser = OldExcelParser(None, fname, sheet=sheet)
                    i = parser.get_data_iter()
                    consistent, ncols = excel_rows_consistent(i())
                    if consistent and ncols > 1:
                        ret.append(i)
                except:
                    _log.info(traceback.format_exc())
        except:
            _log.info(traceback.format_exc())
        return ret


    
if __name__ == '__main__':

    fname = '/Users/sirrice/Desktop/lastpermissiondenied.json'
    parser = DBTruckParser("","")
    readers = parser.get_readers(fname)

    for reader in readers:
        i = 0        
        for r in reader():
            i += 1
        print i
