# DBTruck

The current database import process is like a toilet pipe.  The pipe
easily gets clogged if your data is a bit dirty.

DBTruck turns the import process into a dump truck.  Just throw data
into it, it'll just work.  You can clean it up later!

It currently only works for postgresql.  It assumes that PostgreSQL
is installed and running on your machine, and uses `psql` to load data.

# Usage

## Help me

    python dbtruck.py -h

## Load datafile.txt

    python dbtruck.py datafile.txt tablename dbname