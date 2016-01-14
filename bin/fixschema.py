#!/usr/bin/env python
import readline
import cmd
import pprint
from sqlalchemy import *

from dbtruck.infertypes import infer_col_type


def err_wrapper(fn):
  def f(self, *args, **kwargs):
    if fn.__name__ != "do_connect" and not self.check_conn(): return

    try:
      return fn(self, *args, **kwargs)
    except Exception as e:
      print e
      import traceback
      #traceback.print_exc()
      return None
  f.__name__ = fn.__name__
  f.__doc__ = fn.__doc__
  return f

def pprint(table, padding=2):
  ncols = max(map(len, table))
  for row in table:
    while len(row) < ncols:
      row.append("")

  cols = zip(*table)
  lens = [ max(map(len, map(str, col)))+padding for col in cols ]
  fmts = {}
  for row in table:
    if len(row) not in fmts:
      fmts[len(row)] = ("{:>%s}" * len(row)) % tuple(lens[:len(row) + 1])
    print fmts[len(row)].format(*row)

def tableize(l, padding = 2):
  lens = map(len, map(str, l))
  bestlen = max(lens) + padding
  nitems = max(1, 80 / bestlen)
  table = [[]]
  for v in l:
    if len(table[-1]) >= nitems:
      table.append([])
    table[-1].append(v)
  return table


class SchemaFixer(cmd.Cmd):
  prompt = "> "
  intro = """
  Schema Fixer is a simple interactive prompt for viewing your database
  and renaming attributes in your tables.
  """
  def __init__(self):
    cmd.Cmd.__init__(self, "\t")

    self.dburi = None
    self.engine = None
    self.conn = None

    # cached stuff
    self.tables = []
    self.schemas = {}

    self.cmds = []

  def postloop(self):
    try:
      if self.conn:
        self.conn.close()
    except Exception as e:
      print e
      pass

  def postcmd(self, stop, line):
    self.prompt = "\nContext: %s\n> " % str(self.context)
    return stop

  def completedefault(text, line, begidx, endidx):
    return ["hi"]

  def find_cmd(self, *keys, **d): #*cmds):
    """
    """
    for cmd in reversed(self.cmds):
      if all(k in cmd and cmd[k] == v for k, v in d.items()):
        if all(k in cmd for k in keys):
          return tuple([cmd[k] for k in keys])
    return None

  def __context__(self):
    d = dict()
    for cmd in self.cmds:
      d.update(cmd)
    if 'cmd' in d:
      del d['cmd']
    return d

  context = property(__context__)

  def read_args(self, line, *args):
    inputs = line.split()
    ret = []
    idx = -1
    for idx, (inp, arg) in enumerate(zip(reversed(inputs), reversed(args))):
      ret.append(inp)

    context = self.context
    rargs = list(reversed(args))
    filled_args = []
    filled_vals = []
    for i in xrange(idx+1, len(rargs)):
      arg = rargs[i]
      if isinstance(arg, basestring):
        arg = [arg]

      inp = None
      for sub_arg in arg:
        if sub_arg in context:
          inp = context[sub_arg]
          filled_vals.append(inp)
          filled_args.append(sub_arg)
          break

      if inp is None:
        print "Could not find args %s in context" % ", ".join(rargs[i:])
        return None

    if filled_args:
      pairs = list(reversed(zip(filled_args, filled_vals)))
      print "Args used from Context: %s\n" % ", ".join(["%s = %s" % p for p in pairs])

    ret.extend(filled_vals)
    return tuple(list(reversed(ret)))



  def get_schema(self, table):
    if table not in self.schemas:
      q = """
      SELECT column_name, data_type
      FROM information_schema.columns 
      WHERE table_schema not in ('information_schema', 'pg_catalog')
            AND table_name = %s
      """

      cur = self.conn.execute(q, table)
      pairs = map(tuple, cur)
      self.schemas[table] = pairs
    return self.schemas[table]

  def clear_schema(self, table):
    if table in self.schemas:
      del self.schemas[table]

  def get_inferred_type(self, table, col):
    q = "SELECT distinct %s FROM %s LIMIT 500" % (col, table)
    vals = [row[0] for row in self.conn.execute(q)]
    typ = infer_col_type(vals)
    if typ is None:
      typ = "str"
    elif isinstance(typ, type):
      typ = typ.__name__
    else:
      typ = str(typ)
    return typ

  def print_col_stats(self, table, col, typ):
    q = "SELECT distinct %s FROM %s LIMIT 50" % (col, table)
    vals = [row[0] for row in self.conn.execute(q)]
    print "%s.%s %s\tinferred type %s\n" % (table, col, typ, self.get_inferred_type(table, col))
    pprint(tableize(vals[:50]))
    print



  def emptyline(self):
    pass

  def do_c(self, uri):
    """
    Connect to database using URI.  The latter assumes localhost

    connect [URI]
    connect [tablename]
    """
    return self.do_connect(uri)

  @err_wrapper
  def do_connect(self, uri):
    """
    Connect to database using URI.  The latter assumes localhost

      connect [URI]
      connect [tablename]
    """

    uri = uri.strip()
    if "://" in uri:
      self.dburi = uri
    else:
      self.dburi = "postgresql://localhost/%s" % uri
      print "assuming %s is database name and connecting to %s" % (uri, self.dburi)
    self.engine = create_engine(self.dburi)
    self.conn = self.engine.connect()

  def check_conn(self):
    if self.conn is None:
      print "run connect first:\nconnect <dburi>"
      return False
    return True

  @err_wrapper
  def do_tables(self, line):
    """
    list tables

      tables
    """
    self.cmds.append(dict(cmd="tables"))

    q = """
    SELECT table_name, count(distinct column_name) 
    FROM information_schema.columns 
    WHERE table_schema not in ('information_schema', 'pg_catalog')
    GROUP BY table_name
    ORDER BY table_name
    """

    cur = self.conn.execute(q)
    pairs = [tuple(row) for row in cur]
    self.tables = zip(*pairs)[0]
    tables  = ["{} {:>2}".format(*p) for p in pairs]
    print "printing"
    pprint(tableize(tables, 2))

  @err_wrapper
  def do_cols(self, line):
    """
    list columns in table.  The latter looks for a previously used table argument.

      cols [table]
      cols
    """
    args = self.read_args(line, "table")
    if not args: return
    table, = args
    self.cmds.append(dict(cmd="cols", table=table))

    pairs = self.get_schema(table)

    q = "SELECT %s FROM %s" % (
        ", ".join(["count(distinct %s)" % p[0] for p in pairs]),
        table
    )
    cur = self.conn.execute(q)
    counts = cur.fetchone()

    data = []
    for (col, typ), n in zip(pairs, counts):
      inferred = self.get_inferred_type(table, col)
      data.append((n, typ, col, "inferred %s" % inferred))
    pprint(data)

  def complete_cols(self, text, line, begidx, endidx):
    print "complete cols called"
    if not text:
      return self.tables
    else:
      return [
        t for t in self.tables
        if t.startswith(text)
      ]

  @err_wrapper
  def do_data(self, line):
    """
    Show example data for each column in the table

      data [table]
      data

    """
    args = self.read_args(line, "table")
    if not args: return
    table, = args
    self.cmds.append(dict(cmd="data", table=table))

    paginate = True
    schema = self.get_schema(table)
    for idx, (col, typ) in enumerate(schema):
      self.print_col_stats(table, col, typ)

      if paginate and idx % 3 == 0 and idx > 0:
        try:
          line = raw_input("\n----- press enter for next page, q to exit, a for all data ----")
        except EOFError:
          return
        if line.strip().lower() == "q":
          return
        if line.strip().lower() in ("a", "all"):
          paginate = False

  @err_wrapper
  def do_infer(self, line):
    """
    Infer the data type for specified column

      infer [table] [col]
      infer [col]
      infer
    """
    args = self.read_args(line, "table", "col")
    if not args: return
    table, col = args
    self.cmds.append(dict(cmd="tables", table=table, col=col))
    schema = self.get_schema(table)
    typ = dict(schema).get(col, "str")
    print "%s.%s\t%s inferred type %s" % (table, col, typ, self.get_inferred_type(table, col))

  @err_wrapper
  def do_col(self, line):
    """
    List summary info about column

      col [table] [col]
      col [col]
      col
    """
    args = self.read_args(line, "table", ("newname", "col"))
    if not args: return
    table, col = args

    self.cmds.append(dict(cmd="col", table=table, col=col))
    schema = self.get_schema(table)
    typ = dict(schema).get(col, "str")
    self.print_col_stats(table, col, typ)

  @err_wrapper
  def do_rename(self, line):
    """
    Rename column

      rename [table] [col] [new name]
      rename [col] [new name]
      rename [new name]

    uses the closest matching command to infer the table and col values
    """
    args = self.read_args(line, "table", ("newname", "col"), "newname")
    if not args: return
    table, oldname, newname = args

    if oldname == newname:
      print "Renaming %s to %s is NOOP, skipping" % (oldname, newname)
      return

    self.cmds.append(dict(cmd="rename", table=table, oldname=oldname, newname=newname))

    q = "ALTER TABLE %s RENAME %s TO %s" % (table, oldname, newname)
    self.conn.execute(q)
    self.clear_schema(table)

    print "Renamed %s.%s to %s.%s" % (table, oldname, table, newname)
    print
    self.do_col("%s %s" % (table, newname))

  @err_wrapper
  def do_args(self, line):
    print self.read_args(line, "table", "col")

  @err_wrapper
  def do_type(self, line):
    """
    Change the type of the column.  If type coersion fails, prompts user for explicit
    USING expression

      type [table] [col] [new type]
      type [col] [new type]
      type [new type]

    uses the closest matching command to infer the table and col values
    """
    args = self.read_args(line, "table", "col", "newtype")
    if not args: return
    table, col, newtype = args
    self.cmds.append(dict(cmd="type", table=table, col=col, newtype=newtype))

    template = "ALTER TABLE %s ALTER COLUMN %s TYPE %s " % (table, col, newtype)
    expr = ""
    q = template

    while 1:
      if expr:
        q = "%s USING %s" % (template, expr)
      else:
        q = template

      try:
        self.conn.execute(q)
        print "Set %s.%s to type %s" % (table, col, newtype)
        print
        return self.do_col("%s %s" % (table, col))
      except Exception as e:
        print "Type coersion failed.  Enter an USING expression to try again, or press enter to give up"
        print "  e.g., col1::int"
        try:
          expr = raw_input("USING expression > ")
          if not expr.strip(): 
            print "Giving up"
            return
        except EOFError:
          print "Giving up"
          return


  def do_EOF(self, line):
    return True

if __name__ == '__main__':
    SchemaFixer().cmdloop()