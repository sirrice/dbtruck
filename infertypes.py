import sys, csv
import datetime
from collections import *
from dateutil.parser import parse as dateparse
import math

# notes: alter table readings alter column time type time using (time::time);

def get_type(val):
    val = val.strip()
    if val == '': return None

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
    val = val.strip()
    try:
        if t == datetime.datetime:
            d = dateparse(val).strftime('%Y-%m-%d %H:%M:%S')
            return d
        if t == datetime.date:
            d = dateparse(val).strftime('%Y-%m-%d')
            return d
        if t == datetime.time:
            d = dateparse(val).strftime('%H:%M:%S')
            return d
    except:
        pass

    try:
        if t == int:
            return int(val)
        if t == float:
            return float(val)
    except:
        return 0

    if val == '':
        return 'NULL'
    val = val.replace(',', '')
    return val



def infer_col_types(rowiter):
    """
    @return a list of the most common type for each column
    """
    rowiter.next()
    linenum = 0
    types = [Counter() for j in xrange(max([len(rowiter.next()) for i in xrange(10)]))]
    for row in rowiter:
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
