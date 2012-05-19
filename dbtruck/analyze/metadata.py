import csv
import os
import sys
import pdb
import traceback
sys.path.append(['..', '.', '../exporters'])

from geopy import geocoders
from collections import defaultdict

from dbtruck.exporters.db import *
import dbtruck.settings as settings
            

class LocationMD(object):
    def __init__(self, db):
        self.db = db
        self.MDNAME = '_dbtruck_metadata_loc_'
        try:
            db.execute("""create table %s (
            tablename varchar(128) not null,
            tested bool default false,
            hasloc bool default false,
            maxid int default 0,
            done bool default false)""" % self.MDNAME)
        except:
            pass

    def check_table(self, table):
        try:
            q = """select tablename, tested, hasloc, maxid, done from %s
            where tablename = %%s""" % self.MDNAME
            rows = self.db.execute(q, (table,)).fetchall()
            return rows[0]
        except:
            return [table, False, False, 0, False]

    def is_tested(self, table):
        return self.check_table(table)[1]

    def has_loc(self, table):
        return self.check_table(table)[2]

    def max_id(self, table):
        return self.check_table(table)[3]

    def is_done(self, table):
        return self.check_table(table)[4]

    def rows_left(self, table):
        try:
            maxid = self.max_id(table)
            q = "select count(*) from %s where id > %d" %   (table, maxid)
            return self.db.execute(q).fetchone()[0]
        except:
            return 0
        
    def has_row(self, table):
        try:
            q = "select * from %s where tablename = %%s" %self.MDNAME
            self.db.execute(q, (table,)).fetchone()
            return True
        except:
            return False
                  
        

    def set_table(self, table, **kwargs):
        try:
            if not self.has_row(table):
                kwargs['tablename'] = table
                q = "insert into %s(%s) values(%s)"
                q = q % (self.MDNAME,
                         ','.join(kwargs.keys()),
                         ','.join(['%s'] * len(kwargs)))
                params = tuple(kwargs.values())
                self.db.execute(q, params)
                return True
            else:
                q = "update %s set %s where %s"
                q = q % (self.MDNAME,
                         ','.join(['%s = %%s' % k for k in kwargs.keys()]),
                         'tablename = %s')
                params = list(kwargs.values()) + [table]
                self.db.execute(q, tuple(params))
                return True
        except Exception as e:
            print e
            return False
