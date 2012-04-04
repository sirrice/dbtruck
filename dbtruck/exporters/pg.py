import subprocess, sys, csv, datetime, math, os, logging
from collections import *
from dateutil.parser import parse as dateparse
sys.path.append( os.path.abspath(os.path.dirname(__file__)) )
from infertypes import *
from base import BaseMethods

logging.basicConfig()
_log = logging.getLogger(__file__)


class PGMethods(BaseMethods):

    def __init__(self, *args, **kwargs):
        super(PGMethods, self).__init__(*args, **kwargs)

    def sql_create(self, types, attrs=None, new=True):
        # make up some attribute names
        if attrs is None:
            attrs = ['attr%d' % i for i in xrange(len(types))]
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
        

    def import_block(self, buf):

        with file('/tmp/tmp.load', 'w') as f:
            for row in buf:
                print >>f, ','.join(row)

        with open('/dev/null', 'w') as devnull:
            args = ['psql', '-c', "copy %s from '/tmp/tmp.load' with csv;" % self.tablename, self.dbname]
            ret = subprocess.call(args, stdout=devnull, stderr=devnull)

        if ret == 0:
            return True

        # fall back and insert line by line (or something...)
        if len(buf) > 10:
            block = int(math.ceil(len(buf)/10.0))
            for i in xrange(10):
                self.import_block(buf[block*i:block*(i+1)])
        elif len(buf) > 1:
            for i in xrange(len(buf)):
                self.import_row(buf[i])
        else:
            print >>self.errfile, ','.join(buf[0])
            return False
        return True


    def import_row(self, row):
        return self.import_block([row])


