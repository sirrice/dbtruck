import subprocess
import sys
import csv
import datetime
import math
import os
import logging
import time
import re
import pdb
import traceback

sys.path.append('..')
sys.path.append( os.path.abspath(os.path.dirname(__file__)) )

from collections import *
from dateutil.parser import parse as dateparse
from StringIO import StringIO
from sqlalchemy import *

from dbtruck.util import get_logger, to_utf, block_iter
from dbtruck.infertypes import *
from dbtruck.exporters.base import BaseMethods

_log = get_logger()
copy_re = re.compile('line (?P<line>\d+), column\ (?P<col>\w+): \"(?P<val>.+)\"')
cr_re = re.compile(r'[\r\f\v]')


class PGMethods(BaseMethods):

    def __init__(self, *args, **kwargs):
        super(PGMethods, self).__init__(*args, **kwargs)

        self.dbname = kwargs['dbname']
        self.hostname = kwargs.get('hostname', 'localhost')
        self.username = kwargs.get('user', '')
        self.password = kwargs.get('password', None)
        self.port = kwargs.get('port', 0)

        self.dburi = kwargs.get('uri', None)
        if not self.dburi:
            self.dburi = self.construct_dburi()
            
        self.engine = create_engine(self.dburi)
        self.db = self.engine.raw_connection()

        # haven't decided if storing state in here is a good idea or
        # not, but it's necessary to do error mitigation
        self.attributes = []
        self.types = []

        self.prev_errors = defaultdict(list)
        self.threshold = 10

    def construct_dburi(self):
        dburi = ['postgresql://']
        if self.username:
            dburi.append(self.username)
            if self.password:
                dburi.extend([':', self.password])
            dburi.append('@')
        dburi.append(self.hostname)
        if self.port:
            dburi.append(':%s' % str(self.port))
        dburi.extend(['/', self.dbname])
        return ''.join(dburi)


    def sql_create(self, types, attrs=None, new=True):
        # make up some attribute names
        types = map(BaseMethods.type2str, types)
        stmts = []
        if new:
            cols = []
            for attr, t in zip(attrs, types):
                if attr == 'id':
                    cols.append('id serial unique')
                else:
                    cols.append('%s %s null' % (attr, t))

            drop = 'drop table %s cascade;' % self.tablename
            create = 'create table %s (%s);' % (self.tablename, ', \n'.join(cols))            
            _log.info(create)
            stmts.extend([drop, create])

        self.types = types
        self.attribute = attrs
        return stmts

    def setup_table(self, types, header, new):
        stmts = self.sql_create(types, attrs=header, new=new)    
        for stmt in stmts:
           try:
               self.engine.execute(stmt)
           except:
               _log.info(traceback.format_exc())

    def handle_error(self, errcode, col, val, row):
        """
        This method caches the data that caused import errors in memory, and alters the schema
        to deal with the errors after a threshold number of the same error types have been encountered.

        When the schema is changed to fix an error, the rows that caused the errors are added back 
        into the queue of rows to import

        errors described on http://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
        @return list of rows to re-import
        """
        key = (errcode, col)
        self.prev_errors[key].append( (val, row) )

        if len(self.prev_errors[key]) < self.threshold:
            return False

        _log.info("handling error\t%s", key)
        vals, rows = zip(*self.prev_errors[key])            
        query = None
        if errcode in ['22003']:
            # 22003: NUMERIC VALUE OUT OF RANGE.  change to bigint
            query = "alter table %s alter %s type %s" % (self.tablename, col, 'bigint')
        else:
            # 22001: make column size longer
            # 22007: change column into varchar
            # 22P02: integer column but got string
            newlen = max(64, max(map(len, map(to_utf, vals))) * 2)
            newtype = 'varchar(%d)' % newlen if newlen <= 1024 else 'text'
            query = "alter table %s alter %s type %s" % (self.tablename, col, newtype)


        if query:
            self.engine.execute(query)
            
            del self.prev_errors[key]
            # import the rows related to the error that we just fixed!            
            return rows
        return None
            
    def prepare_row_for_copy(self, row):
        newrow = []
        for col in row:
            if col is None:
                newrow.append('NULL')
            elif isinstance(col, basestring):
                newrow.append(cr_re.sub('\r', to_utf(col).replace('\t', ' ')))
            else:
                newrow.append(str(col).replace('\t', ' '))


        if len(newrow) < len(self.types):
            newrow += ['NULL'] * (len(self.types) - len(newrow))
        if len(newrow) > len(self.types):
            newrow = newrow[:len(self.types)]
        return newrow
                

    def run_copy(self, buf):
        s = StringIO()
        w = csv.writer(s, delimiter='\t')
        w.writerows(map(self.prepare_row_for_copy, buf))
        s.seek(0)

        error = None
        start = time.time()
        try:
            cur = self.db.cursor() 
            cur.copy_from(s, self.tablename, sep='\t', null='NULL')
            self.db.commit()
            cur.close()
            return None # good
        except Exception as e:
            error = e
            self.db.rollback()
        finally:
            s.close()
        return error
            
    def import_block(self, buf, iterf):
        bufs = [buf]

        # get rid of the recursion, if possible
        while len(bufs) > 0:

            cur_buf = bufs.pop(0)
            if not cur_buf:
                continue
            error = self.run_copy(cur_buf)
            if not error:
                continue

            error_args = copy_re.findall(error.message)
            if error_args:
                errcode = error.pgcode
                line, col, val = error_args[0]
                try:
                    pos = iterf.header.index(col)
                except:
                    pdb.set_trace()
                    print col, iterf.header
                line = int(line) - 1
                row = cur_buf[line]
                val = row[pos]
                old_err_rows = self.handle_error(errcode,
                                                 col,
                                                 val,
                                                 row)
                if old_err_rows:
                    bufs.insert(0, old_err_rows)

                if len(cur_buf) <= 1:
                    row = cur_buf[0]
                    normrow = map(to_utf, row)
                    print >>self.errfile, ','.join(normrow)
                    # print row
                    # print errcode, error
                    # print col
                    # print val
                    continue
                bufs.append(cur_buf[:line])

                # usually the next N rows will be bad, so do them
                # individually
                for i in xrange(1,100,20):
                    bufs.append(cur_buf[line+i:line+i+20])
                bufs.append(cur_buf[line+i+20:])
                
                _log.debug( "error\t%s\t%d\t%s\t%s",
                            errcode, line, col, val )
                _log.debug( error )
            else:
                # default to recursively trying 
                _log.warn("couldn't parse error in '%s'\t%s", str(error), error.pgcode)
                if len(cur_buf) > 10:
                    map(bufs.append, block_iter(cur_buf, 10))
                elif len(cur_buf) > 1:
                    #map(self.import_row, cur_buf)
                    for buf in cur_buf:
                        bufs.append([buf])
                else:
                    row = cur_buf[0]
                    normrow = map(to_utf, row)
                    print >>self.errfile, ','.join(normrow)

        return True


    def import_row(self, row):
        try:
            args = ','.join(["%s"] * len(row))
            query = "insert into %s values (%s)" % (self.tablename, args)
            self.engine.execute(query, tuple(row))
            return None
        except Exception as e:
            error = e
            normrow = map(to_utf, row)
            print >>self.errfile, ','.join(normrow)
            #_log.warn("import row error\t%s", e)
            return error


