# DBTruck
[Eugene Wu](http://www.sirrice.com)

The current database import process is like a toilet pipe.  The pipe
easily gets clogged if your data is a bit dirty.  You gotta clean up the data, figure out the schema, figure out the types, etc etc.  It really sucks!  I once spent 2.5 hours importing two datasets from the [FEC](http://www.fec.gov/disclosurep/pnational.do).

`DBTruck` is meant to turn the import process into a dump truck.  Just throw data
into it, should just work!  Who cares about attribute names!  Who cares about types you don't care about!  You can clean it up later!

It assumes that your file is one tuple per line.  Other than that, it
will:

* Automatically split up each line in a way that makes sense
* Try to interpret each column's type
  * Currently supports `int`, `float`, `date`, `time`, `timestamp`
  * Defaults to `varchar(100)`
* Make fake attribute names (`attr0`,…,`attrN`) so you don't need to
* Import the file in blocks so that a single bad row doesn't blow the
  whole operation
  * It'll even pinpoint the specific bad rows and log them to an error
    file 


`DBTruck` assumes that `PostgreSQL` is installed and running on your 
machine, and uses `psql` to load data.

# Usage

    python dbtruck.py data/testfile.txt tablename dbname
    python dbtruck.py -h

# Comments

If there are uses you would like to see, let me know!  I'm adding features for what
I want, but I'm interested in other uses.


Lots and lots of todos!

