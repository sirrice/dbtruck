import datetime

class BaseMethods(object):
    
    def __init__(self, tablename, dbname, errfile):
        self.tablename = tablename
        self.dbname = dbname
        self.errfile = errfile

    def sql_create(self, types, attrs=None):
        raise

    def setup_table(self, types, header):
        raise

    def import_block(self, buf):
        raise 

    def import_row(self, row):
        raise


    @staticmethod
    def type2str(t):
        if t == datetime.datetime:
            return 'timestamp'
        if t == datetime.date:
            return 'date'
        if t == datetime.time:
            return 'time'
        if t == int:
            return 'int'
        if t == float:
            return 'float'
        return 'varchar(100)'
