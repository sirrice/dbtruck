import csv
import os
import sys
import numpy as np
import pdb
import traceback
sys.path.extend(['..', '.', '../exporters'])

from geopy import geocoders
from collections import defaultdict
from scipy.stats import pearsonr

from models import *
from load_data import *
from hidden import *
from dbtruck.exporters.db import *
import dbtruck.settings as settings




def get_correlations(db):#, t1, t2):
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

    keep track of table that have
    1) been tested for joinability
    2) the joins to use

    invalidate table pairs where
    1) latlon or shape data has changed

    keep track of
    - column pairs to be compared
      - within tables
      - between tables and the joins to use

    compute correlations on
    - ordered subsets of the data
    - samples
    """
    meta = MetaData(db)
    meta.reflect()
    tablenames = meta.tables.keys()
    tablemds = [get_table_metadata(db, tn) for tn in tablenames]
    tablemds = filter(lambda md: md.state == 5, tablemds)
    
    tablestats = []
    for tablemd in tablemds:
        table = tablemd.tablename
        radius = compute_radius(db, table)
        bshape = has_shape(db, table)
        tablestats.append((tablemd, radius, bshape))
        print table, radius, bshape
    
    res = []
    for idx, (tmd1, r1, s1) in enumerate(tablestats):
        for tmd2, r2, s2 in tablestats[idx+1:]:

            if len(db_session.query(CorrelationPair).filter(
                CorrelationPair.md1 == tmd1,
                CorrelationPair.md2 == tmd2
                ).all()):
                continue

            
            t1, t2 = tmd1.tablename, tmd2.tablename
            r = np.mean([r1, r2])
            djoinres = dist_join(db, t1, t2, r)

            for corr, p, col1, col2 in test_correlation(djoinres):
                colname1 = col1[1]
                agg1 = col1[2] if len(col1) > 2 else None
                colname2 = col2[1]
                agg2 = col2[2] if len(col2) > 2 else None
                cp = CorrelationPair(corr, colname1, agg1, colname2, agg2, tmd1, tmd2)
                res.append(cp)
    return res
            #loc_join = run_join(db, t1, t2, r1, r2, s1, s2, 'loc_join')
            #agg_join = run_join(db, t1, t2, r1, r2, s1, s2, 'aggregate_loc_join')
            #test_correlation(loc_join)
            #test_correlation(agg_join)


def test_correlation(join):
    # 2 el tuples can be correlated together
    # 2 el tuples can only be correlated with 3 el tuples from other table
    ret = []
    if not join:
        return ret
    
    cols = join[0].keys()
    for idx, col1 in enumerate(cols):
        for col2 in cols[idx:]:
            if col1 == col2: continue
            try:
                cd1 = [row[col1] for row in join]
                cd2 = [row[col2] for row in join]
                cd1 = map(lambda v: v and float(v) or 0, cd1)
                cd2 = map(lambda v: v and float(v) or 0, cd2)
                coef, p = pearsonr(cd1, cd2)
                ret.append((coef, p, col1, col2))
            except:
                import pdb
                pdb.set_trace()
    return ret


def run_join(db, t1, t2, r1, r2, s1, s2, prefix):
    if s1 and s2:
        raise Exception()
        suffix = "3(db, t1, t2)"
    elif s1:
        raise Exception()
        suffix = "2(db, t2, t1, r2)"
    elif s2:
        raise Exception()
        suffix = "2(db, t1, t2, r1)"
    else:
        suffix = "1(db, t1, t2, r1, r2)"

    cmd = "res = %s_%s" % (prefix, suffix)
    exec(cmd)
    return res
        
    

### Same zoom level

def dist_join(db, t1, t2, r):
    # given radius r1, r2, find overlapping lat lons
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    if not sels:
        return []
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]

    q = """select count(*) from %s, %s
           where (%s._latlon <-> %s._latlon) < %%s 
           """ % (t1, t2, t1, t2)
    count = db.execute(q, (r,)).fetchone()[0]
    thresh = 5000. / count

    q = """select %s from %s, %s
           where (%s._latlon <-> %s._latlon) < %%s and
           random() <= %%s
           """ % (','.join(sels), t1, t2, t1, t2)
    res = db.execute(q, (r,thresh)).fetchall()
    return [dict(zip(cols, row)) for row in res]
    



### Same zoom level

def loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2, find overlapping lat lons
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    if not sels:
        return []
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     circle(%s._latlon, %%s) &&
     circle(%s._latlon, %%s) and
     %s._latlon is not null and
     %s._latlon is not null
    """ % (','.join(sels), t1, t2, t1, t2, t1, t2)

    res = db.execute(q, (r1, r2)).fetchall()
    return [dict(zip(cols, row)) for row in res]    

def loc_join_2(db, t1, t2, r1):
    # given radius r1, and t2 has shape file
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    if not sels:
        return []
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     circle(point(%s.latitude, %s.longitude), %%s) &&
     %s.shape""" % (','.join(sels), t1, t2, t1, t1, t2)
    
    res = db.execute(q, (r1, )).fetchall()
    return [dict(zip(cols, row)) for row in res]    

def loc_join_3(db, t1, t2):
    # given both shape files
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = map(lambda s: '%s.%s' % (t1, s), t1cols)
    sels2 = map(lambda s: '%s.%s' % (t2, s), t2cols)
    sels = sels1 + sels2
    if not sels:
        return []
    cols = [(t1, c) for c in t1cols] + [(t2, c) for c in t2cols]
    q = """select %s from %s, %s where
     %s.shape &&  %s.shape""" % (','.join(sels), t1, t2, t1, t2)

    res = db.execute(q).fetchall()
    return [dict(zip(cols, row)) for row in res]    


### Different zoom level -- t1 contains t2

def check_agg_loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2
    q = """select %s.id, count(*) from %s, %s where
      circle (point(%s.latitude, %s.longitude), %%s) ~
      circle (point(%s.latitude, %s.longitude), %%s)
      group by %s.id""" % (t1, t1, t2, t1, t1, t2, t2, t1)
    return db.execute(q, (r1, r2)).fetchall()

def check_agg_loc_join_2(db, t1, t2, r1):
    # given radius r1, t2.shape
    q = """select %s.id, count(*) from %s, %s where
      circle (point(%s.latitude, %s.longitude), %%s) ~ %s.shape
      group by %s.id""" % (t1, t1, t2, t1, t1, t2, t1)
    return db.execute(q, (r1,)).fetchall()

def check_agg_loc_join_3(db, t1, t2):
    # t1.shape, t2.shape
    q = """select %s.id, count(*) from %s, %s where
      %s.shape ~ %s.shape
      group by %s.id""" % (t1, t1, t2, t1, t2, t1)
    return db.execute(q).fetchall()


def get_numeric_columns(db, table, ignore=[]):
    data_types = ['smallint', 'integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision']
    args = ','.join(['%s'] * len(data_types))
    ignore_str = 'and not(column_name in (%s))' % ','.join(['%s']*len(ignore)) if ignore else ''
    q = """select column_name
    from information_schema.columns
    where table_name = %%s and column_name != 'latitude' and
          column_name != 'longitude' and data_type in (%s)
          %s""" % (args, ignore_str)
    cols = db.execute(q, tuple([table] + data_types + ignore)).fetchall()
    cols = [c[0] for c in cols]
    return cols
    

def aggregate_loc_join_1(db, t1, t2, r1, r2):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]
    if not sels:
        return []

    subq = """select %s.id, %s
    from %s, %s as t2
    where circle(%s._latlon, %%s) &&
          circle(%s._latlon, %%s) and
          %s._latlon is not null and
          %s._latlon is not null
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t2, t1, t2)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = db.execute(q, (r1, r2)).fetchall()
    return [dict(zip(sels, row)) for row in res]


def aggregate_loc_join_2(db, t1, t2, r1):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]
    if not sels:
        return []

    subq = """select %s.id, %s
    from %s, %s as t2
    where circle(%s._latlon, %%s) && t2.shape and
          %s._latlon is not null
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t1, t1)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = db.execute(q, (r1,)).fetchall()
    return [dict(zip(sels, row)) for row in res]


def aggregate_loc_join_3(db, t1, t2):
    # given radius r1, r2
    aggs = ['stddev', 'avg', 'min', 'max', 'count', 'sum']
    t1cols = get_numeric_columns(db, t1, ignore=['id'])
    t2cols = get_numeric_columns(db, t2, ignore=['id'])
    sels1 = ['%s.%s' % (t1, c) for c in t1cols]
    sels2 = ['%s(t2.%s) as %s_%s' % (agg, c, c, agg)
             for c in t2cols for agg in aggs]
    sels = [(t1, c) for c in t1cols] + [(t2, c, agg) for c in t2cols for agg in aggs]
    if not sels:
        return []

    subq = """select %s.id, %s
    from %s, %s as t2
    where %s.shape && t2.shape and
    %s.shape is not null and t2.shape is not null
    group by %s.id""" % (t1, ','.join(sels2), t1, t2, t1, t1, t1)

    q = """select %s, subq.* from %s, (%s) as subq
    where subq.id = %s.id;""" % (','.join(sels1), t1, subq, t1)
    
    res = db.execute(q).fetchall()
    return [dict(zip(sels, row)) for row in res]



def compute_radius(db, table):
    """Computing radius sizes for a table"""
    # ath.erf(0.063 / (2 ** 0.5))
    # 5% of distribution falls within 0.063 * stddev
    q = """select stddev(_latlon[0]), stddev(_latlon[1])
    from %s where _latlon is not null""" % table
    latstd, lonstd = db.execute(q).fetchone()
    #return 0.063 * (latstd + lonstd) / 2.
    return 0.013 * (latstd + lonstd) / 2.

def has_shape(db, table):
    try:
        q = "select count(*) from %s where shape is not null" % table
        return db.execute(q).fetchone()[0] > 0
    except:
        return False



if __name__ == '__main__':
    from sqlalchemy import *
    from sqlalchemy.orm import scoped_session, sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    from database import *

    #db = create_engine('postgresql://sirrice@localhost:5432/test')
    init_db()


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
    res = get_correlations(db)
    res.sort(key=lambda cp: cp.corr, reverse=True)

    for cp in res:
        print cp
        db_session.add(cp)
    db_session.commit()

