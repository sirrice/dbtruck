import csv
import os
import json
import re


re_nonwords = re.compile(r'\W')


def get_zipcodes():
    dirname = os.path.dirname(os.path.abspath(__file__))
    zips = {}
    states = set()
    with file(os.path.join(dirname, '../data/us-zipcodes.csv'), 'r') as f:
        r = csv.reader(f)
        for row in r:
            data = {'lat' : row[5],
                    'lon' : row[4],
                    'state' : row[2].lower(),
                    'city' : row[3].lower()}
            zips[row[1].lower()] = data
            states.add(row[2].lower())
    return zips

def get_states():
    dirname = os.path.dirname(os.path.abspath(__file__))
    data = json.load(file(os.path.join(dirname, '../data/us-state-names.json'), 'r'))
    ret = {}
    for row in data:
        name = row[0]
        for v in row:
            if v and not v.startswith("("):
                v = re_nonwords.sub('', v)
                if v:
                    ret[v] = name
    return ret

if __name__ == '__main__':
    print get_states().items()[:2]
    print get_zipcodes().items()[:2]
