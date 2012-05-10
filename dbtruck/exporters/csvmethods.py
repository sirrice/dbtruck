import csv

from base import *

class CSVMethods(BaseMethods):
    def __init__(self, *args, **kwargs):
        BaseMethods.__init__(self, *args, **kwargs)
        self.outfile = None
        self.writer = None
        self.header = None

    def setup_table(self, types, header, new=False):
        self.outfile = file(self.tablename, 'w' if new else 'a')
        self.writer = csv.DictWriter(self.outfile, header)
        self.header = header

    def import_block(self, buf, iterf):
        dicts = (dict(zip(self.header, row)) for row in buf)
        self.writer.writerows(dicts)

    def import_row(self, row):
        try:
            self.writer.writerow(dict(zip(self.header, row)))
        except:
            print row

    def close(self):
        self.outfile.close()
