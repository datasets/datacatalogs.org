#!/usr/bin/env python
import os
import ConfigParser
import urllib
import json
import time

config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)),
    'process.cfg')
config = ConfigParser.SafeConfigParser()
config.read([config_file])
geonames_username = config.get('geonames', 'username')
geonames_baseurl = 'http://api.geonames.org/searchJSON?maxRows=1&username=%s&q=' % geonames_username

if not os.path.exists('cache'):
    os.makedirs('cache')

if not os.path.exists('cache/datacatalogs.json'):
    url = 'http://datacatalogs.org/api/search/dataset?q=&limit=500&all_fields=1'
    urllib.urlretrieve(url, 'cache/datacatalogs.json')

if not os.path.exists('cache/datacatalogs.geocoded.json'):
    num_geocoded = 0
    num_failed = 0
    rawtext = open('cache/datacatalogs.json', 'r').read()
    datasets = json.loads(rawtext)
    geocoded_datasets = []
    for dataset in datasets['results']:
        spatial_text = dataset['extras']['spatial_text']
        spatial_text = spatial_text.encode('utf8', 'ignore')
        url = geonames_baseurl + urllib.quote(spatial_text)
        fo = urllib.urlopen(url)
        res = fo.read()
        res = json.loads(res)
        if res['geonames']:
            dataset['spatial'] = res['geonames'][0]
            print "Geocoded dataset {name}".format(**dataset)
            num_geocoded = num_geocoded + 1
        else:
            print "No geonames in result for dataset {name}".format(**dataset)
            num_failed = num_failed + 1
        time.sleep(0.5)
    json.dump([dataset for dataset in datasets['results'] if 'spatial' in dataset],
            open('cache/datacatalogs.geocoded.json', 'w'))
    print "Geocoded {0} datasets, {1} failed".format(num_geocoded, num_failed)
else:
    # TODO
    pass
