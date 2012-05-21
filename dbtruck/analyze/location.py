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
sys.path.extend(['..', '.', '../exporters'])

from geopy import geocoders
from collections import defaultdict

from load_data import *
#from hidden import *
#from fuzzyjoin import *
#from metadata import *
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

re_badchar = re.compile(r'[^\w\,\.\-\(\)]')

ZIPCODES = get_zipcodes()
STATES = get_states()
COUNTYNAMES = []
FIPS = []

def parse_default(v):
    return v

def parse_coords(v):

    try:
        newv = re.sub('[^\w]', 'X', v)        
        absdlat, absmlat, absslat = re_coord_w.search(newv).groups()
        absdlon, absmlon, absslon = re_coord_n.search(newv).groups()
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
        newv = re.sub('[^\d\-\.]', ' ', v).strip()
        lat, lon = map(float, newv.split())
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


