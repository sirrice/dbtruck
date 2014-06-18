import sys
import csv
import datetime
import math
import os
import logging
import re
import time
import pdb
moduledir = os.path.abspath(os.path.dirname(__file__)) 
sys.path.append( moduledir )

from collections import *
from dateutil.parser import parse as dateparse

from infertypes import *
from parsers.parsers import *
from util import get_logger, to_utf


_log = get_logger()


def file_iterators(fnames, parser=None, **kwargs):
  if not parser:
    parser = DBTruckParser('/tmp/', '/tmp/')

  if isinstance(fnames, basestring):
        fnames = [fnames]

  iterfs = parser.get_reader(fnames)
  iterfs = [iterf for iterf in iterfs]
  for iterf in iterfs:
    iterf.infer_metadata()
  return iterfs


def import_datafiles(fnames, new, tablename, errfile, exportmethodsklass, parser=None, **kwargs):
  """
  Parse 

  Args:
    fnames: file names of files to parse and import
    new: drop table(s) before importing? 
    tablename: prefix of table to import into
    errfile:
    exportmethodsklass: static class containing export methods
  """
  _log.info("kwargs: %s", kwargs)

  iterfs = file_iterators(fnames, parser=parser, **kwargs)

  new_errfile = False
  if not errfile:
    errfile = file('/dev/null', 'a')
    new_errfile = True

  try:
    for idx, iterf in enumerate(iterfs):
      try:
        exportmethods = exportmethodsklass(tablename, errfile, **kwargs)

        newtable = new and idx == 0
        exportmethods.setup_table(iterf.types, iterf.header, newtable)
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
  # The following code turned out to be too expensive to run
  # val = map(validate_type, zip(types, row))
  # if reduce(lambda a,b: a and b, val):
  #     return row
  # return None
  

def import_iterator(iterf, dbmethods):
  """
  """
  # this function could dynamically increase or decrease the block
  rowiter = iterf()
  types = iterf.types
  blocksize = 100000
  buf = []

  if iterf.header_inferred:
    rowiter.next()

  rowidx = dbmethods.get_max_id() or 0

  for row in rowiter:
    rowidx += 1

    row = transform_and_validate(types, row)

    if iterf.add_id_col:
      row.append(str(rowidx))

    if row is not None and len(row) == len(iterf.types):
      buf.append(row)
    elif row is not None and len(row) != len(iterf.types):
      print >>dbmethods.errfile, ','.join(map(to_utf, row))

    if len(buf) > 0 and len(buf) % blocksize == 0:
      try:
        _log.info("flushing %s rows", len(buf))
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
    


