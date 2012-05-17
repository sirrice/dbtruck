import csv
import os
import sys
import pdb
import traceback
sys.path.append(['..', '.', '../exporters'])

from geopy import geocoders
from collections import defaultdict

from load_data import *
from hidden import *
from dbtruck.exporters.db import *
import dbtruck.settings as settings




def get_join_candidates(db):#, t1, t2):
    """
    Types of joins that make sense
    Location
    - same zoom level
      - nearest neighbor with radius
    - different zoom level
      - within
      - aggregate (avg, std, max, count)
    Time
    - same hour, same day, same day of week, same month, same day of month

    Location: has shape file, no shape file
    """
    tablestats = []
    for table in get_hidden_table_names(db):
        radius = compute_radius(db, table)
        bshape = has_shape(db, table)
        tablestats.append((table, radius, bshape))

    res = []
    for idx, (t1, r1, s1) in enumerate(tablestats):
        for t2, r2, s2 in tablestats[idx+1:]:
            print t1, t2
            loc_join = run_join(db, t1, t2, r1, r2, s1, s2, 'loc_join')
            agg_join = run_join(db, t1, t2, r1, r2, s1, s2, 'aggregate_loc_join')
            res.append((t1, t2, loc_join, agg_join))
    return res


def run_join(db, t1, t2, r1, r2, s1, s2, prefix):
    if s1 and s2:
        suffix = "3(db, t1, t2)"
    elif s1:
        suffix = "2(db, t2, t1, r2)"
    elif s2:
        suffix = "2(db, t1, t2, r1)"
    else:
        suffix = "1(db, t1, t2, r1, r2)"

    cmd = "res = %s_%s" % (prefix, suffix)
    exec(cmd)
    return res
        
    



### Same zoom level

def loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2, find overlapping lat lons
    t1cols, t2cols = get_numeric_columns(db, t1), get_numeric_columns(db, t2)
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     circle(point(%s.latitude, %s.longitude), %%s) &&
     circle(point(%s.latitude, %s.longitude), %%s)
    """ % (','.join(sels), t1, t2, t1, t1, t2, t2)

    res = query(db, q, (r1, r2))
    return [dict(zip(cols, row)) for row in res]    

def loc_join_2(db, t1, t2, r1):
    # given radius r1, and t2 has shape file
    t1cols, t2cols = get_numeric_columns(db, t1), get_numeric_columns(db, t2)
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     circle(point(%s.latitude, %s.longitude), %%s) &&
     %s.shape""" % (','.join(sels), t1, t2, t1, t1, t2)
    
    res = query(db, q, (r1, ))
    return [dict(zip(cols, row)) for row in res]    

def loc_join_3(db, t1, t2):
    # given both shape files
    t1cols, t2cols = get_numeric_columns(db, t1), get_numeric_columns(db, t2)
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     %s.shape &&  %s.shape""" % (','.join(sels), t1, t2, t1, t2)

    res = query(db, q)
    return [dict(zip(cols, row)) for row in res]    


### Different zoom level -- t1 contains t2

def check_agg_loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2
    q = """select %s.id, count(*) from %s, %s where
      circle (point(%s.latitude, %s.longitude), %%s) ~
      circle (point(%s.latitude, %s.longitude), %%s)
      group by %s.id""" % (t1, t1, t2, t1, t1, t2, t2, t1)
    return query(db, q, (r1, r2))

def check_agg_loc_join_2(db, t1, t2, r1):
    # given radius r1, t2.shape
    q = """select %s.id, count(*) from %s, %s where
      circle (point(%s.latitude, %s.longitude), %%s) ~ %s.shape
      group by %s.id""" % (t1, t1, t2, t1, t1, t2, t1)
    return query(db, q, (r1,))

def check_agg_loc_join_3(db, t1, t2):
    # t1.shape, t2.shape
    q = """select %s.id, count(*) from %s, %s where
      %s.shape ~ %s.shape
      group by %s.id""" % (t1, t1, t2, t1, t2, t1)
    return query(db, q)


def get_numeric_columns(db, table):
    data_types = ['smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision']
    args = ','.join(['%s'] * len(data_types))
    q = """select column_name
    from information_schema.columns
    where table_name = %%s and column_name != 'latitude' and
          column_name != 'longitude' and data_type in (%s)""" % (args)
    cols = query(db, q, tuple([table] + data_types))
    cols = [c[0] for c in cols]
    return cols
    

def aggregate_loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1)
    t2cols = get_numeric_columns(db, t2)
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]

    subq = """select %s.id, %s
    from %s, %s as t2
    where circle(point(%s.latitude, %s.longitude), %%s) &&
          circle(point(t2.latitude, t2.longitude), %%s)
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t1, t1)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = query(db, q, (r1, r2))
    return [dict(zip(sels, row)) for row in res]


def aggregate_loc_join_2(db, t1, t2, r1):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1)
    t2cols = get_numeric_columns(db, t2)
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]

    subq = """select %s.id, %s
    from %s, %s as t2
    where circle(point(%s.latitude, %s.longitude), %%s) &&
          t2.shape
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t1, t1)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = query(db, q, (r1,))
    return [dict(zip(sels, row)) for row in res]


def aggregate_loc_join_3(db, t1, t2):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1)
    t2cols = get_numeric_columns(db, t2)
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]

    subq = """select %s.id, %s
    from %s, %s as t2
    where t1.shape && t2.shape
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = query(db, q)
    return [dict(zip(sels, row)) for row in res]



def compute_radius(db, table):
    """Computing radius sizes for a table"""
    # ath.erf(0.063 / (2 ** 0.5))
    # 5% of distribution falls within 0.063 * stddev
    q = """select stddev(latitude), stddev(longitude)
    from %s
    where latitude is not null and longitude is not null""" % table
    latstd, lonstd = query(db, q)[0]
    return 0.063 * (latstd + lonstd) / 2.

def has_shape(db, table):
    try:
        q = "select count(*) from %s where shape is not null" % table
        return query(db, q)[0][0] > 0
    except:
        return False



if __name__ == '__main__':

    db = connect('test')
    t1, t2 = 'test2', 'test1'
    r1, r2 = 40, 5
    # print loc_join_1(db, t1, t2, r1, r2)
    # print loc_join_2(db, t1, t2, r1)
    # print loc_join_3(db, t1, t2)
    # print check_agg_loc_join_1(db, t1, t2, r1, r2)
    # print check_agg_loc_join_2(db, t1, t2, r1)
    # print check_agg_loc_join_3(db, t1, t2)
    # print aggregate_loc_join_1(db, t1, t2, r1, r2)
    # print aggregate_loc_join_2(db, t1, t2, r1)
    # print aggregate_loc_join_3(db, t1, t2)
    # print compute_radius(db, t1)
    # print compute_radius(db, t2)
    ress = get_join_candidates(db)
    for res in ress:
        print res[:2]
        print res[2][0].values()
        print res[3][0].values()
    db.close()
