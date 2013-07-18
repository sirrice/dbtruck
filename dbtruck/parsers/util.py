import os
import sys
import csv
import logging
import datetime
import math
import re
import xlrd
import pdb
import traceback

from operator import and_
from collections import *
from pyquery import PyQuery
from openpyxl import load_workbook
from dateutil.parser import parse as dateparse

from ..util import get_logger

_log = get_logger()


def rows_consistent(iter):
    lens = get_row_counts(iter)
    if not lens:
        return False, 0
    maj = lens.most_common(1)[0][1]
    tot = sum(lens.values())
    return float(maj) / tot, lens.most_common(1)[0][0]
    return float(maj)/tot >= 0.98, lens.most_common(1)[0][0]

def html_rows_consistent(iter):
    lens = get_row_counts(iter)
    if not lens:
        return False, 0
    maj = lens.most_common(1)[0][1]
    tot = sum(lens.values())
    consistent = float(maj)/tot >= 0.6 and tot > 2
    return consistent, lens.most_common(1)[0][0]


def excel_rows_consistent(iter):
    lens = get_row_counts(iter)
    if not lens:
        return False, 0
    maj = lens.most_common(1)[0][1]
    tot = sum(lens.values())
    consistent = float(maj)/tot >= 0.6 and tot > 2
    return consistent, lens.most_common(1)[0][0]
    

def get_row_counts(iter):
    """
    iterates through first 10k rows in iterator and records the number of
    columns in each row.  Checks that >98% of the rows have the same number of columns
    @return (isconsistent, number of columns)
    """
    try:
        idx = 0
        iter.next()
        lens = Counter()
        for idx, row in enumerate(iter):
            lens[len(row)] += 1
            if idx > 10000: break
        return lens
    except:

        _log.info(traceback.format_exc())
        print "Iteration ended at row %d" % (idx+1)



# Deprecated thanks to csv.Sniffer
#
# def _get_reader(f, delim):
#     """
#     creates an CSV based iterator
#     """
#     f.seek(0)
#     f = (line.strip() for line in f)
#     if delim is None:
#         reader = csv.reader(f)
#     elif len(delim) == 1:
#         reader = csv.reader(f, delimiter=delim)
#     else:
#         reader = (r.split(delim) for r in f)
# 
#     try:
#         while True:
#             try:
#                 yield reader.next()
#             except StopIteration:
#                 break
#             except Exception as e:
#                 _log.info(traceback.format_exc())                
#     except:
#         _log.info(traceback.format_exc())                




def is_url(fname, **kwargs):
    if fname.startswith('http://') or fname.startswith('www.'):
        return True

def is_url_file(fname, **kwargs):
    try:
        with file(fname, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                if not is_url(line):
                    return False
        return True
    except Exception as e:
        _log.debug(e)
        return False

def is_html_file(fname, **kwargs):
    try:
        size = os.path.getsize(fname)
        if size > 1048576 * 4:
            return False
        with file(fname, 'r') as f:
            return len(PyQuery(f.read())('table')) > 0
    except Exception as e:
        _log.debug(e)
        return False

def is_excel_file(fname, **kwargs):
    try:
        load_workbook(fname)
        return True
    except:
        return False


def is_old_excel_file(fname, **kwargs):
    try:
        xlrd.open_workbook(fname)
        return True
    except:
        return False
