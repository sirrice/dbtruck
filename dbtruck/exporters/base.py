import datetime

class BaseMethods(object):
    
    def __init__(self, tablename, errfile, **kwargs):
        self.tablename = tablename
        self.errfile = errfile

    def setup_table(self, types, header, new=False):
        raise

    def import_block(self, buf, iterf):
        raise 

    def import_row(self, row):
        raise

    def close(self):
        pass
    
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
        return 'varchar(128)'
