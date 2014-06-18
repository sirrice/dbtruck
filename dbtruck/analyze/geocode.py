import pickle
import re
import bsddb

from geopy import geocoders

from dbtruck.util import to_utf

re_addr2 = re.compile(r'^\s*\d{1,6}(\s+[a-zA-Z\-]+){1,3}\s*$')
def distance_func(pt1, pt2):
    return ((float(pt1[0]) - float(pt2[0])) ** 2 +
            (float(pt1[1]) - float(pt2[1])) ** 2) ** 0.5

class DBTruckGeocoder(object):
    def __init__(self, fname='/tmp/geocache.bdb'):
        try:
            self.cache = bsddb.hashopen(fname)
        except:
            self.cache = {}
        try:
            self.ncalls = int(self.cache.get('__ncalls__', '0'))
        except:
            self.ncalls = 0

    def close(self):
        try:
            self.cache.close()
        except:
            pass

    def geocode(self, address, restriction):
        try:
            locs = self._geocode(address, restriction)
            rlocs = self._geocode(restriction)
            if locs:
                if rlocs:
                    restriction_latlon = rlocs[0][1]
                    locs.sort(key=lambda loc: distance_func(loc[1], restriction_latlon))

                description, (lat, lon) = locs[0]
                query = self.get_format_string(restriction) % address

                return description, (lat, lon), query
        except KeyboardInterrupt:
            return None
        except Exception as e:
            raise
        return None

    def get_format_string(self, restriction):
        if restriction.strip():
            return '%s, '  + restriction.strip()
        return '%s'

    def _geocode(self, address, restriction=''):
        """
        Try to pick the best geocoder for the address.  Google has a low
        daily limit so try to use other geocoders if possible

        If the address looks like a standard address (NUMBER WORDS+),
        then use yahoo, bing, geocoder.us

        Otherwise if it is a place name, try geonames

        If all else fails, use Google
        """
        geocoder = None
        format_string = self.get_format_string(restriction)
        query = to_utf((format_string % address).lower())

        if not query:
            return []

        if query in self.cache:
            try:
                return pickle.loads(self.cache[query])
            except:
                pass
        
        if re_addr2.search(address) and restriction:
            rand = random.random()
            if rand < 0.4:
                geocoder = geocoders.Yahoo(settings.YAHOO_APPID)
            elif rand < 0.8:
                geocoder = geocoders.Bing(settings.BING_APIKEY)
            else:
                geocoder = geocoders.GeocoderDotUS()
        else:
            geocoder = geocoders.GeoNames()

        try:
            result = geocoder.geocode(query, exactly_one=False)
            self.ncalls += 1
        except:
            try:
                geocoder = geocoders.Google()
                result = geocoder.geocode(query, exactly_one=False)
                self.ncalls += 1
            except:
                result = []

        self.cache[query] = pickle.dumps(result)
        return result

if __name__ == '__main__':
    geocoder = DBTruckGeocoder()

    print geocoder.geocode('cayuga', 'New York')
    print geocoder.geocode('cayuga', 'New York')
    print geocoder.geocode('cayuga', 'New York')    
    print geocoder.ncalls
