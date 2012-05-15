import csv
import os
import sys
sys.path.append(['..', '.', '../exporters'])

from geopy import geocoders
from collections import defaultdict

from load_data import *
from dbtruck.exporters.db import *
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
re_addr2 = re.compile(r'^\s*\d+(\s+[a-zA-Z\-]+){1,3}\s*$')

ZIPCODES = get_zipcodes()
STATES = get_states()
COUNTYNAMES = []
FIPS = []
"""
construct hidden tables that contain lat/lon info for each table
 - table: t
 - hidden: _t_loc_
   - lat
   - lon
   - query
   - address
   - city
   - state
   - country
   - zipcode
detect states, cities and use to do better geocoding job
if 'lat' and 'lon' columnames exist and 95% of values between normal ranges
then call 

"""

def possible_loc(colname, coltype, vals):
    def is_ok(new_vals, maxposs=vals, thresh=0.65):
        return len(filter(lambda s: s, new_vals)) > thresh * len(maxposs)

    nonempty = [v for v in vals if v]        
    colname = colname.lower().strip()

    if 'lat' in colname:
        lats = []
        for v in vals:
            try:
                v = float(v)
                if -90 < v and v < 90:
                    lats.append(v)
                else:
                    lats.append(None)
            except:
                lats.append(None)
        if is_ok(lats, nonempty, thresh=0.8):
            return {'latitude' : lats}

    if 'lon' in colname:
        lons = []
        for v in vals:
            try:
                v = float(v)
                if -180 < v and v < 180:
                    lons.append(v)
                else:
                    lons.append(None)
            except:
                lons.append(None)
        if is_ok(lons, nonempty, thresh=0.8):
            return {'longitude' : lons}


    vals = [ re_nonwords.sub(' ', str(v)).lower().strip() for v in vals if v]
    nonempty = [v for v in vals if v]

    # maybe its a composite of latitude longitude
    failures = 0.
    lats, lons = [], []
    for v in vals:
        try:
            lat, lon = map(float, v.split())
            if -90 < lat and lat < 90 and -180 < lon and lon < 180:
                lats.append(lat)
                lons.append(lon)
        except:
            lats.append(None)
            lons.append(None)
            failures += 1
            if failures > 0.5 * len(lats) + 10:
                break
    if is_ok(lats, nonempty, thresh=0.8):
        return {'latitude' : lats, 'longitude' : lons}

    # zipcodes
    if 'zip' in colname:
        zips = [ZIPCODES.get(v, None) for v in vals]
        if is_ok(zips, nonempty):
            return {"zipcode" : zips}
            
    # states
    if colname.startswith('st'):
        states = [STATES.get(v, None) for v in vals]
        if is_ok(states, nonempty):
            return {'state' : states}


    # county codes

    # countries
    
    # street addresses (number string string suffix)
    # column is not a single attribute, lets look for composite data
    # ok maybe its embedded in the text??
    ret = {}
        
    allwords = [v.split() for v in vals]
    states = []
    zips = []
    for words in allwords:
        ss = filter(lambda s: s, [STATES.get(w, None) for w in words])
        states.append(ss[0] if ss else None)

        zz = filter(lambda z:z, [ZIPCODES.get(w, None) for w in words])
        zips.append(zz[0] if zz else None)
            
    if is_ok(states, nonempty, thresh=0.8):
        ret['state'] = states
    if is_ok(zips, nonempty, thresh=0.8):
        ret['zipcode'] = zips

    addrs = [re_addr2.search(v) for v in vals if len(v) < 100]
    if is_ok(addrs, nonempty):
        addrs = [m and m.string[m.start():m.end()] or m for m in addrs]
        ret['address'] = addrs
    else:
        addrs = [re_addr1.search(v) for v in vals if len(v) < 100]
        import pdb
        pdb.set_trace()
        
        if is_ok(addrs, nonempty):
            nums = [re_addr1_num.search(v) for v in vals if len(v) < 100]
            addrs = [a and n for a, n in zip(addrs, nums)]
            if is_ok(addrs, nonempty):
                ret['address'] = vals

    # cross streets (string* suffix ?? string* suffix)
    return ret


def get_loc_stats(db, table, colname):
    """
    count
    centroid
    avg distance --> zoom?
    stddev distance
    """
    pass

def geocodeit(addr):
    domain = 'http://maps.googleapis.com/maps/api/geocode/json?'
    import requests


def get_latlon(address=None, state=None, zipcode=None, city=None, latitude=None, longitude=None):
    if not address:
        return []
    if latitude and longitude:
        return zip(address, zip(latitude, longitude))

    user_input = None
    if not state and not zipcode and not city:
        try:        
            ui = raw_input('can you enter a city,state or zip? ')
            if ui:
                user_input = [ui] * len(address)
            else:
                raw_input("probably can't do a good job without more info.  <enter> to continue")
        except KeyboardInterrupt:
            return None


    ret = []
    for idx, s in enumerate(address):
        s = s.strip()
        if not re_nonwords.sub('', s):
            ret.append(None)
            continue
        
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
            if not locs:
                ret.append(None)
            else:
                ret.append(locs[0])
                print s, locs[0]
        except KeyboardInterrupt:
            break
        except:
            ret.append(None)
        if idx > 10:
            break
    return ret

def get_zoomlevel(data):
    "is this just avg distance between two points?"
    pass



def get_join_candidates(db):
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
    """
    pass



def create_hidden_table(db, table):
    try:
        loctable = '_%s_loc_' % table

        # create if it doesn't exist
        q = """create table %s (
             id int references %s.id,
             lat float null,
             lon float null,
             query text null,
             address text null,
             city varchar(128) null,
             state varchar(128) null,
             country varchar(128) null,
             desc text null,
             zoom float null,
             shape polygon null        
        )""" % (loctable, table)
        prepare(db, q)
        return True
    except:
        return False


def get_hidden_table_names(db):
    try:
        q = """select table_name
               from information_schema.tables
               where table_schema = 'public' and table_type = 'BASE TABLE'"""
        for table_name, in query(db, q):
            if table_name.startswith('_') and table_name.endswith('_loc_'):
                yield table_name
    except:
        pass


def analyze_loc_in_table(db, table):
    res = query(db, 'select * from %s limit 4000' % table)
    data = zip(*res)
    cols = query(db, "select column_name, data_type from information_schema.columns where table_name = '%s' order by ordinal_position asc" % table)

    loc_dict = defaultdict(list)
    
    for (colname, coltype), vals in zip(cols, data):

        ret = possible_loc(colname, coltype, vals)
        for key, vals in ret.items():
            loc_dict[key].append(vals)
            print '\t', colname, key, '\t', vals[10:20]

    return loc_dict

def annotate_with_loc(db, table, loc_dict):
    if 'address' in loc_dict:
        loc_dict = dict([(key, vals[0]) for key, vals in loc_dict.items()])
        lat_lons = get_latlon(**loc_dict)
        return lat_lons
    return None
                

if __name__ == '__main__':

    db = connect('test')
    tables = query(db, "select table_name from information_schema.tables where table_schema = 'public';")
    for table, in [('addresses',)]:#tables:
        print table
        
        loc_dict = analyze_loc_in_table(db, table)
        #annotate_with_loc(db, table, loc_dict)
                    
    db.close()

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

