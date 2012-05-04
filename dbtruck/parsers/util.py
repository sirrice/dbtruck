import sys, csv, logging, datetime, math, re
from operator import and_
from collections import *
from dateutil.parser import parse as dateparse

logging.basicConfig()
_log = logging.getLogger(__file__)


def rows_consistent(iter):
    """
    iterates through first 10k rows in iterator and records the number of
    columns in each row.  Checks that >98% of the rows have the same number of columns
    @return (isconsistent, number of columns)
    """
    try:
        idx = 0
        iter.next()
        lens = Counter()
        for idx, row in enumerate(iter):
            lens[len(row)] += 1
            if idx > 10000: break
        maj = lens.most_common(1)[0][1]
        tot = sum(lens.values())
        return float(maj)/tot >= 0.98, lens.most_common(1)[0][0]
    except:
        import traceback
        traceback.print_exc()
        print "Iteration ended at row %d" % (idx+1)


def _get_reader(f, delim):
    """
    creates an CSV based iterator
    """
    f.seek(0)
    f = (line.strip() for line in f)
    if delim is None:
        reader = csv.reader(f)
    else:
        reader = csv.reader(f, delimiter=delim)

    try:
        while True:
            try:
                yield reader.next()
            except StopIteration:
                break
            except Exception as e:
                _log.error(str(e))
    except:
        pass

