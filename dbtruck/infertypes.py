import sys
import csv
import re
import math
import datetime

from collections import *
from dateutil.parser import parse as dateparse


# notes: alter table readings alter column time type time using (time::time);

re_null_chars = re.compile('[\*\.\?-_]+')

def get_type(val):
    if not isinstance(val, basestring):
        return type(val)
    val = val.strip()
    if not val: return None

    try:
        i = int(val)
        return int
    except:
        pass
    try:
        f = float(val)
        return float
    except:
        pass

    try:
        d = dateparse(val)
        if d.hour == d.minute and d.minute == d.microsecond and d.hour == 0:
            return datetime.date
        elif datetime.datetime.now().date() == d.date():
            if d.hour != 0 or d.minute != 0 or d.second != 0 or d.microsecond != 0:
                return datetime.time
        return datetime.datetime
    except:
        pass

    return str

def validate_type((t, v)):
    """
    check that the value matches the type
    """
    if v == 'NULL':
        return True
    
    if t in [datetime.datetime, datetime.date, datetime.time]:
        if '0000-00-00' == v:
            return False
        
    try:
        if t in [int, float]:
            t(v)
    except:
        return False
    
    return True

def str2sqlval((t, val)):
    if not isinstance(val, basestring):
        return val
    if issubclass(t, basestring):
        return val
    
    val = val.strip()
    nullval = re_null_chars.sub('', val)
    # try:
    #     if t == datetime.datetime:
    #         d = dateparse(val).strftime('%Y-%m-%d %H:%M:%S')
    #         return d
    #     if t == datetime.date:
    #         d = dateparse(val).strftime('%Y-%m-%d')
    #         return d
    #     if t == datetime.time:
    #         d = dateparse(val).strftime('%H:%M:%S')
    #         return d
    # except:
    #     pass

    try:
        if t == int:
            return int(val)
        if t == float:
            return float(val)
    except:
        if not nullval:
            return None
        return 0

    if not nullval:
        return None

    return val



def infer_col_types(iterf):
    """
    @return a list of the most common type for each column
    """
    # infer best row length
    if iterf.header:
        types = [Counter() for j in xrange(len(iterf.header))]
    else:
        rowiter = iterf()
        rowiter.next()
        counter = Counter(len(rowiter.next()) for i in xrange(1000))
        bestrowlen = counter.most_common(1)[0][0]
        types = [Counter() for j in xrange(bestrowlen)]


    rowiter = iterf()
    linenum = 0        
    for row in rowiter:
        if len(row) != len(types):
            continue
        
        for key, val in enumerate(row):
            t = get_type(val)
            if t is not None: 
                types[key][t] += 1
        linenum += 1
        if linenum > 5000:
            break
    commons =  [c.most_common(1) for c in types]
    commons = [len(c) and c[0][0] or str for c in commons]
    return commons
