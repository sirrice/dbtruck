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

# Requirements

* Python
* [dateutil](http://labix.org/python-dateutil#head-2f49784d6b27bae60cde1cff6a535663cf87497b)
* [openpyxl](http://packages.python.org/openpyxl/usage.html)
* [PostgreSQL](http://www.postgresql.org/download/) running locally
* psql


# Installation

I recommend installing dbtruck in a virtualenv environment.  You can use pip to install dbtruck:

    pip install dbtruck

This will install two scripts into your path -- `importmydata.py` and `fixschema.py`.  
The former is used to import data files into the database, and the latter can be used
to quickly look at data in your database and rename/retype attributes.

# Usage

For importmydata:

    importmydata.py -h
    importmydata.py data/testfile.txt tablename dbname

Running fixschema opens a prompt that supports commands.  The following is an example of
a session using fixschema:

      fixschema.py

      Schema Fixer is a simple interactive prompt for viewing your database
      and renaming attributes in your tables.


      Context: {}
      > connect test
      assuming test is database name and connecting to postgresql://localhost/test

      Context: {}
      > tables
      printing
                          a  2           actedin  2            actors  3
                          b  3  csp_perrow_aug10 11  csp_perrow_oct14 11
        csp_perrow_oct14_agg 12               fec 18            ground 31
                      india 31            movies  3               pcm 32
                    sailors  1            states  2             times  3
                          us 31          zipcodes  9

      Context: {}
      > cols times
        227  character varying        wid    inferred str
          1  character varying  hittypeid    inferred str
        323   double precision          t  inferred float

      Context: {'table': 'times'}
      > col wid
      Args used from Context: table = times

      times.wid character varying     inferred type str

        A9S1BY9MWXXZGBV  AT83AJM75Q8NNC8  A898X2BT3PN03LR  AXFM9RGKY2I26U5
        AONOWZ7O27B0SG2  A5X4LXE3QF81IQJ  AFO8DX2JVVP1D08  A4F7CZYSA1HQGKO
        A5Z3RNTYZGXY02B  AEW8WKVAMFOXNPU  ANQKKE0T4K7M6IN  AE2XCOMRJP77LAL
        A6X5UAD2CJC3ZKM  AHPTFIPLCBKL59H  AX599ENLZL403QB  ATW3FTREUWEY7LV
        A3XP57KTJAZNSJ8  AI86KEPNR7B8BBT  AXM7QGPWTS887I1  AAX804A3G334LW8
        AB54WA1G2ACEOFN  ADT3GN8G9KKCJHK  A4IB1WR87KI34GD  A1N18Y83VOHEWBD
        AYJFL6YB1JG8KP2  ARF4T3MT5AEQVXP  AX1TCT4R31BI6YO  AZKCX0XVAHSU3JS
        AK1ACNTS5LJLMGQ  AP1BBGGSM9QKBEZ  AVF8T1U29Z9MNPS  AK8THL0QCYKHDF7
        AL3K2PL54YS6LGY  AU8JXZYMNKFMLBQ  AZ9MHGKX5SKGVL3  AS5MC33Q619AI02
        A1Z3L2G9L3HBKN1  ADQQ8Y0TI0OFYZ1  AE3EARS17OIZTCZ  AGWMH5U0ZGY385N
        AMPLNJJO7VC9B0Z  AMZDPTPDGW7IJJI  ATA0N1WYQOOTIB6  AN06A258WERMYXN
        A5M2VW4YNF1D8HB  AFPBCW7DQ204SL0  A6WO8Y1C422PBUN  AXY45VP2SHRDU0E
        A6K48N7EQQV0MFK  AJL60SXZWS2H9R4


      Context: {'table': 'times', 'col': 'wid'}
      >


# TODOs

Immediate ToDos

* ~~Faster import: lots of datafiles have errors scattered throughout the data, which dramatically
  slows down bulk inserts.~~ 
  * ~~Do preliminary filtering for errors~~
  * ~~Fall back to (prepared) individual inserts once too many bulk insert attempts fail~~
* Better error reporting
  * Load failed data into a hidden table in the database
  * Log error reasons
  * Try to recover from typical errors (date column contains a random string) by using reasonable defaults
* ~~Refactor file iterator objects to keep track of hints identified earlier in the pipeline~~
  * for example, parsed json files can infer that the dictionary keys are table headers -- no
    need to re-infer that later in the pipeline
  * Include confidence scores for each inference
* ~~Support extracting multiple tables from each input file~~
  * an HTML file may contain multiple tables to be imported
* ~~Support downloading URLS and HTML files~~
* ~~Support CSV output~~
* ~~Support Excel Files~~


## Comments

If there are uses you would like to see, let me know!  I'm adding features for what
I want, but I'm interested in other uses.

In the future I would like to add

* Good geocoder for location columns
  * support for simple location joins
* support for other databases
* let you specify port/host etc
* support additional data file types (json, fixed offset, serialized)
* support renaming and reconfiguring the tables after the fact
* inferring foreign key relationships
* creating indexes
* interactive interface instead of requiring command line flags
* and more!
