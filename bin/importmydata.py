#!/usr/bin/env python
import logging

from dbtruck.dbtruck import *
from dbtruck.util import get_logger
from dbtruck.exporters.pg import PGMethods
from dbtruck.exporters.csvmethods import CSVMethods

if __name__ == '__main__':

    import click


    @click.command()
    @click.option('--err', default='/tmp/errlines.txt',
                  help='destination to log rows thare have errors')
    @click.option('--log', default='./dbtruck.log',
                  help='destination to log debug messages')
    @click.option('--append', is_flag=True, 
                  help='replace existing table or append to it')
    @click.option('-d', is_flag=True,
                  help='print debug messages to stdout')
    @click.option("--dest", default="pg", type=click.Choice(['pg', 'csv']),
                  help="Output to database or cleaned csv file (not ready)")
    @click.option('--user', default="sirrice",
                  help='database user (if applicable)')
    @click.option('--host', default="localhost",
                  help='database host (if applicable)')
    @click.option('--port', default=5432, type=int,
                  help='database port (if applicable)')
    @click.option('--password', default="", type=str,
                  help="database password (if applicable)")
    @click.option("--dburi", default=None,
                  help="database URI (preempts any other db options)")
    @click.argument('tablename', nargs=1)
    @click.argument('dbname', nargs=1)
    @click.argument('filenames', nargs=-1)
    def main(err, log, append, d, dest,
            user, host, port, password, dburi,
            tablename, dbname, filenames):

      filenames = list(filenames)

      with file(err, 'w') as errfile:
          plevel = logging.DEBUG if d else logging.WARNING
          _log = get_logger(fname=log, plevel=plevel)
          dbsettings = dict(
              dbname = dbname,
              hostname = host,
              username = user,
              port = port,
              password = password,
              uri = dburi
          )

          import_datafiles(filenames, not append, tablename, errfile, PGMethods,
                          **dbsettings)
          errfile.close()

    main()

    # potential options
    # new = T/F
    # errfile = ... file to print rows with errors
    # 


