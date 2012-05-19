# location.py
# construct hidden tables that contain lat/lon info for each table
#  - table: t
#  - hidden: _t_loc_
#    - lat
#    - lon
#    - query
#    - address
#    - city
#    - state
#    - country
#    - zipcode
# detect states, cities and use to do better geocoding job
# if 'lat' and 'lon' columnames exist and 95% of values between normal ranges
# then call 

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
from fuzzyjoin import *
from metadata import *
from dbtruck.exporters.db import *
from dbtruck.util import to_utf
import dbtruck.settings as settings


addr_suffixes = ['av(e(nue)?)?',
                 'st(reet)?',
                 'ct',
                 'court',
                 'rd',
                 'road',
                 'dr(ive)?',
                 'ln',
                 'lane',
                 '((h)?wy|(high)?way)(\s*\d+)?'
                 'blvd',
                 'boulevard',
                 'circle',
                 '\d+(rd|th|nd|st)']
re_addr1 = re.compile(r'\s+(%s)\w{0,50}$' % '|'.join(addr_suffixes))
re_addr1_num = re.compile(r'\w?\d+(\s+|[^rd|th|nd|st])')
re_addr2 = re.compile(r'^\s*\d{1,6}(\s+[a-zA-Z\-]+){1,3}\s*$')
re_addrx = re.compile(r'(([a-zA-Z\-]+){1,3}\s+(%s)(\s+|$)){2}' % '|'.join(addr_suffixes))

re_coord_w = re.compile(r'(?P<a>\d+)X(?P<b>\d+)X(?P<c>\d+\.?\d+?)X?[wW]')
re_coord_n = re.compile(r'(?P<a>\d+)X(?P<b>\d+)X(?P<c>\d+\.?\d+?)X[nN]')

re_badchar = re.compile(r'[^\w\.\-\(\)]')

ZIPCODES = get_zipcodes()
STATES = get_states()
COUNTYNAMES = []
FIPS = []



def parse_coords(v):
    try:
        v = re.sub('[^\w]', 'X', v)
        absdlat, absmlat, absslat = re_coord_w.search(v).groups()
        absdlon, absmlon, absslon = re_coord_n.search(v).groups()
        latsign = absdlat < 0 and -1 or 1
        lonsign = absdlon < 0 and -1 or 1
        absdlat = abs(absdlat * 1000000.)
        absmlat = abs(absmlat * 1000000)
        absslat = abs(absslat * 1000000)
        absdlon = abs(absdlon * 1000000.)
        absmlon = abs(absmlon * 1000000)
        absslon = abs(absslon * 1000000)
        
        lat = (absdlat + absmlat/60. + absslat/3600.) * latsign/1000000.
        lon = (absdlon + absmlon/60. + absslon/3600.) * lonsign/1000000.
        return lat, lon
    except:
        pass
    try:
        lat, lon = map(float, v.split())
        if -90 < lat and lat < 90 and -180 < lon and lon < 180:
            return lat, lon
    except:
        pass
    
    return None, None

def parse_lat(v):
    try:
        v = float(v)
        if -90 < v and v < 90:
            return v
    except:
        pass
    return None
def parse_lon(v):
    try:
        v = float(v)
        if -180 < v and v < 180:
            return v
    except:
        pass
    return None
def parse_zip(v):
    return ZIPCODES.get(v, None)
def parse_state(v):
    return STATES.get(v, None)
def parse_per_word_state(v):
    ss = filter(lambda s:s, map(parse_state, v.split()))
    return ss[0] if ss else None
def parse_per_word_zip(v):
    ss = filter(lambda s:s, map(parse_zip, v.split()))
    return ss[0] if ss else None
def parse_addrs(v):
    if len(v) < 100:
        m = re_addr2.search(v)
        if m:
            return v
            #return m.string[m.start():m.end()]
        m = re_addrx.search(v)
        if m:
            return v
            #return m.string[m.start():m.end()]
        m = re_addr1.search(v)
        n = re_addr1_num.search(v)
        if m and n:
            return v
            #return m.string[m.start():m.end()]
    return None


def possible_loc(colname, vals):
    def is_ok(new_vals, maxposs=vals, thresh=0.65):
        n = 0
        for v in new_vals:
            if isinstance(v, list) or isinstance(v, tuple):
                if filter(lambda s:s, v):
                    n += 1
            else:
                if v != None:
                    n += 1
        if float(n) > thresh * len(maxposs):
            return n
        return False

    vals = [ re_badchar.sub(' ', to_utf(v)).lower().strip() for v in vals if v]
    nonempty = [v for v in vals if v]        
    colname = colname.lower().strip()
    ret = {}#defaultdict()
    
    if 'lat' in colname:
        lats = map(parse_lat, vals)
        if is_ok(lats, nonempty, thresh=0.8):
            ret['latitude'] = 'parse_lat'

    if 'lon' in colname:
        lons = map(parse_lon, vals)
        if is_ok(lons, nonempty, thresh=0.8):
            ret['longitude'] = 'parse_lon'

    if 'latitude' in ret and 'longitude' in ret:
        return ret

    if is_ok(map(parse_coords, vals), nonempty, thresh=0.5):
        ret['latlon'] = 'parse_coords'
        return ret



    if 'zip' in colname:
        zips = map(parse_zip, vals)
        if is_ok(zips, nonempty):
            return {"zipcode" : 'parse_zip'}
            
    if colname.startswith('st'):
        states = map(parse_state, vals)
        if is_ok(states, nonempty):
            return {'state' : 'parse_state'}

    zips = map(parse_per_word_zip, vals)
    if is_ok(zips, nonempty, thresh=0.8):
        ret['zipcode'] = 'parse_per_word_zip'

    states = map(parse_per_word_state, vals)
    if is_ok(states, nonempty, thresh=0.8):
        ret['state'] = 'parse_per_word_state'


    # county codes
    # countries

    # street addresses (number string string suffix)
    # column is not a single attribute, lets look for composite data
    # ok maybe its embedded in the text??
    addrs = map(parse_addrs, vals)
    if is_ok(addrs, nonempty, thresh=0.55):
        ret['address'] = 'parse_addrs'
    return ret


def get_latlon_annotations(address=None, **kwargs):
    """
    Annotate addresses with as much information as we can get our
    hands on...
    """
    if 'id' not in kwargs:
        return None
    
    state = kwargs.get('state', None)
    zipcode = kwargs.get('zipcode', None)
    city = kwargs.get('city', None)
    latitude = kwargs.get('latitude', None)
    longitude = kwargs.get('longitude', None)
    user_input = kwargs.get('user_input', None)
    maxinserts = kwargs.get('maxinserts', None)
    

    annotations = [{'id' : x} for x in kwargs['id']]
    for key, vals in kwargs.items():
        if vals and isinstance(vals, list) or isinstance(vals, tuple):
            for anno, val in zip(annotations, vals):
                anno[key] = val
    
    if latitude and longitude:
        return annotations

    if not state and not zipcode and not city:
        if not user_input:
            try:        
                ui = raw_input('can you enter a city, state or zip? ')
                if ui:
                    user_input = ui
                else:
                    raw_input("probably can't do a good job without more info.  <enter> to continue")
            except KeyboardInterrupt:
                return None
        user_input = [user_input] * len(address)
    else:
        user_input = None

    ret = []
    for idx, s in enumerate(address):
        if not s: continue
        s = s.strip()
        if not re_nonwords.sub('', s):
            continue

        anno = annotations[idx]
        
        restriction = []
        if city:
            restriction.append(city[idx])
        if state:
            restriction.append(state[idx])
        if zipcode:
            restriction.append(zipcode[idx])
        if user_input:
            restriction.append(user_input[idx])

        restriction = ' , '.join(restriction)
        if restriction:
            format_string = '%s, '  + restriction
        else:
            format_string = '%s'
            
        
        g = geocoders.Yahoo(settings.YAHOO_APPID, format_string=format_string)
        
        try:
            locs = g.geocode(s, exactly_one=False)
            if locs:
                loc = locs[0]
                print '\t', s, '\t', loc
                anno.update({'query' : format_string % s,
                             'description' : loc[0],
                             'latitude' : loc[1][0],
                             'longitude' : loc[1][1]})
        except KeyboardInterrupt:
            break
        except Exception as e:
            print e

        if maxinserts and idx > maxinserts:
            break
    return annotations






def find_location_columns(db, table, maxid=0):
    cols = db.execute("""select column_name, data_type from
    information_schema.columns where table_name = '%s' order by
    ordinal_position asc""" % table).fetchall()
    if not cols:
        return {}, {}
    colnames, coltypes = zip(*cols)

    arg = ','.join(colnames)
    res = db.execute("""select %s from %s where id >= %s order by id asc
                       limit 1500""" % (arg, table, maxid)).fetchall()
    data = zip(*res)

    loc_dict = defaultdict(list)
    colname_dict = defaultdict(list)
    print "find location of", table
    for colname, coltype, vals in zip(colnames, coltypes, data):

        ret = possible_loc(colname, coltype, vals)

        for key, vals in ret.items():
            if vals:
                loc_dict[key].append(vals)
                colname_dict[key].append(colname)
                print '\t', colname, key


    loc_dict['id'] = [data[colnames.index('id')]]
    return loc_dict, colname_dict

def get_location_annotations(db, loc_dict, **kwargs):
    loc_dict = dict([(key, vals[0]) for key, vals in loc_dict.items()])
    kwargs.update(loc_dict)
    lat_lons = get_latlon_annotations(**kwargs)
    return lat_lons


def insert_annotations(db, table, annotations, **kwargs):
    maxinserts = kwargs.get('maxinserts', None)
    attrs = get_table_cols(db, table)
    q = 'insert into %s values (%s);' % (table, ','.join(['%s']*len(attrs)))
    maxid = None
    for idx, anno in enumerate(annotations):
        try:
            params = [anno.get(attr, None) for attr in attrs]
            db.execute(q, tuple(params))
            maxid = anno['id']
            if maxinserts and idx > maxinserts:
                break
        except Exception as e:
            print e
    return maxid

def create_and_populate_location_table(db, regtable, **kwargs):
    locmd = LocationMD(db)
    maxid = 0

    if locmd.is_tested(regtable):
        if locmd.has_loc(regtable):
            maxid = locmd.max_id(regtable)
            loc_dict, colname_dict = find_location_columns(
                db,
                regtable,
                maxid=maxid)
            
            annotations = get_location_annotations(
                db,
                loc_dict,
                **kwargs)
            loctable = create_hidden_table(db, regtable)            
            maxid = insert_annotations(db, loctable, annotations,
                                       **kwargs)
            if maxid is not None:
                locmd.set_table(regtable, maxid=maxid)
        else:
            return
    else:
        loc_dict, colname_dict = find_location_columns(db, regtable)
        locmd.set_table(regtable, tested=True)
        if len(loc_dict) > 1:
            locmd.set_table(regtable, hasloc=True)            

            annotations = get_location_annotations(db, loc_dict, **kwargs)
            loctable = create_hidden_table(db, regtable)
            if loctable is None:
                raise Exception("could not create %s" % regtable)

            # drop annotations into new table
            maxid = insert_annotations(db, loctable, annotations, **kwargs)
            if maxid:
                locmd.set_table(regtable, maxid=maxid)
        else:
            locmd.set_table(regtable, hasloc=False)


def join_regular_and_hidden_tables(db, regtable):
    try:
        loctable = '_%s_loc_' % regtable
        regcols = get_table_cols(db, regtable)
        loccols = get_table_cols(db, loctable)
        try:
            loccols.remove('id')
        except:
            pass

        sel1, sel2, sels = [], [], []
        for col in regcols:
            if col in loccols:
                continue
            sel1.append('%s.%s as %s' % (regtable, col, col))
            sels.append(col)
        sel1 = ','.join(sel1)

        for col in loccols:
            sel2.append('%s.%s as %s' % (loctable, col, col))
            sels.append(col)
        sel2 = ','.join(sel2)


        q = "select %s, %s from %s, %s where %s.id = %s.id"
        q = q % (sel1, sel2, regtable, loctable, regtable, loctable)
        rows = db.execute(q).fetchall()
        return [dict(zip(sels, row)) for row in rows]
    except:
        traceback.print_exc()
        return None

def analyze_all_tables(db):
    for regtable, loctable in get_table_name_pairs(db):
        if not loctable:
            create_and_populate_location_table(db, regtable)

if __name__ == '__main__':
    from sqlalchemy import *
    from sqlalchemy.orm import scoped_session, sessionmaker
    from sqlalchemy.ext.declarative import declarative_base

    db = create_engine('postgresql://sirrice@localhost:5432/test')

    
    # for regtable, loctable in get_table_name_pairs(db):
    #     print regtable, loctable
    #     if regtable and loctable:
    #         rows = join_regular_and_hidden_tables(db, regtable)
    #         if rows:
    #             print rows[:10]
    # db.close()
    # exit()
    #print get_table_name_pairs(db)
    #print join_regular_and_hidden_tables(db, 'test')[1]

    #exit()
    #loc_dict, colname_dict = find_location_columns(db, 'buildings_3')
    create_and_populate_location_table(db, 'tacobell', maxinserts=30,
                                       locdata=None)
    exit()


    db = connect('mattdata')


    with open("dump.csv", "w") as f:
        w = csv.DictWriter(f, ['neighborhood', 'fullname', 'lat', 'lon'])
        w.writeheader()
        g = geocoders.Google()
        for neigh, in query(db, "select distinct neighborhood from boscrime;"):
            try:
                fullname, (lat, lon) = g.geocode(neigh)
                w.writerow({'neighborhood' : neigh,
                            'fullname' : fullname,
                            'lat' : lat,
                            'lon' : lon})
            except Exception as e:
                print e
                print neigh
    db.close()

