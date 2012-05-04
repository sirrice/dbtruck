import subprocess
import sys
import csv
import datetime
import math
import os
import logging
import time
import re

sys.path.append('..')
sys.path.append( os.path.abspath(os.path.dirname(__file__)) )

from collections import *
from dateutil.parser import parse as dateparse
from StringIO import StringIO

from util import block_iter
from infertypes import *
from db import connect
from base import BaseMethods

logging.basicConfig()
_log = logging.getLogger(__file__)
copy_re = re.compile('line (?P<line>\d+), column\ (?P<col>\w+): \"(?P<val>.+)\"')


class PGMethods(BaseMethods):

    def __init__(self, *args, **kwargs):
        super(PGMethods, self).__init__(*args, **kwargs)
        self.db = connect(self.dbname)

        # haven't decided if storing state in here is a good idea or
        # not, but it's necessary to do error mitigation
        self.attributes = []
        self.types = []

        self.prev_errors = defaultdict(list)
        self.threshold = 30
        

    def sql_create(self, types, attrs=None, new=True):
        # make up some attribute names
        types = map(BaseMethods.type2str, types)
        stmts = []
        if new:
            create = ',\n'.join(['%s %s null' % (attr, t) for attr, t in zip(attrs, types)])
            create = 'create table %s (%s);' % (self.tablename, create)
            drop = 'drop table %s;' % self.tablename
            stmts.extend([drop, create])
        sql = '\n'.join(stmts)
        return sql

    def setup_table(self, types, header, new):
        createsql = self.sql_create(types, attrs=header, new=new)    
        with file('/tmp/tmp.create', 'w') as f:
            print >>f, createsql
        subprocess.call(['psql', '-f', '/tmp/tmp.create', self.dbname])
        

    def handle_error(self, errcode, col, val, row):
        key = (errcode, col)
        self.prev_errors[key].append( (val, row) )

        if len(self.prev_errors[key]) < self.threshold:
            return False

        vals, rows = zip(*self.prev_errors[key])            
        query = None
        if errcode in ['22001', '22007']:
            # 22001: make column size longer
            # 22007: change column into varchar
            newlen = max(map(len, vals)) * 2
            newlen = max(newlen, 128)
            newtype = 'varchar(%d)' % newlen if newlen <= 1024 else 'text'
            query = "alter table %s alter %s type %s" % (self.tablename, col, newtype)
        elif errcode in ['22003']:
            # 22003: NUMERIC VALUE OUT OF RANGE.  change to bigint
            query = "alter table %s alter %s type %s" % (self.tablename, col, 'bigint') 
        else:
            #raise "error code not recogniszed %s" % errcode
            pass


        if query:
            cur = self.db.cursor()
            cur.execute(query)
            self.db.commit()
            cur.close()

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
                enc = col.encode('utf-8', errors='ignore')
                newrow.append(enc.replace('\t', ' '))
            else:
                newrow.append(str(col).replace('\t', ' '))
        return '\t'.join(newrow)
                

    def run_copy(self, buf):
        s = StringIO('\n'.join(map(self.prepare_row_for_copy, buf)))
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
                pos = iterf.header.index(col)
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
                    print >>self.errfile, ','.join(map(str, row))
                    print errcode, error
                    print col
                    print val
                    continue
                bufs.append(cur_buf[:line])

                # usually the next N rows will be bad, so do them
                # individually
                for i in xrange(1,100,20):
                    bufs.append(cur_buf[line+i:line+i+20])
                bufs.append(cur_buf[line+i+20:])
                
                _log.debug( "error\t%dt%d\t%s\t%s",
                            errcode, line, col, val )
                _log.debug( error )
            else:
                # default to recursively trying 
                print "couldn't parse error in ", str(error)
                if len(cur_buf) > 10:
                    map(bufs.append, block_iter(cur_buf, 10))
                elif len(cur_buf) > 1:
                    map(self.import_row, cur_buf)
                else:
                    row = cur_buf[0]
                    print >>self.errfile, ','.join(map(str, row))
                    print error
                    print row

        return True


    def import_row(self, row):
        try:
            cur = self.db.cursor()
            args = ','.join(["%s"] * len(row))
            query = "insert into %s values (%s)" % (self.tablename, args)
            cur.execute(query, tuple(row))
            self.db.commit()
            cur.close()
            return None
        except Exception as e:
            error = e
            self.db.rollback()
            print >>self.errfile, ','.join(map(str, row))
            print e
            print row
            return error


