#!/usr/bin/env python
import os
import ConfigParser
import urllib
import json
import time
import datetime

config_file = os.path.join(os.path.dirname(os.path.dirname(__file__)),
    'process.cfg')
config = ConfigParser.SafeConfigParser()
config.read([config_file])
geonames_username = config.get('geonames', 'username')
geonames_baseurl = 'http://api.geonames.org/searchJSON?maxRows=1&username=%s&q=' % geonames_username
time_of_last_geonames_request = datetime.datetime.now()

def geonames_lookup(spatial_text):
    '''
    Return the lat. and long. from a geonames search for the given text.

    Results from geonames are cached in a local file.

    :param spatial_text: the text to search for, e.g. "Albania:
    :type spatial_text: string

    :rtype: a dictionary with keys 'lat' and 'long', or None if the geonames
        search fails

    '''
    global time_of_last_geonames_request
    if os.path.exists('cache/geonames.json'):
        cache = json.loads(open('cache/geonames.json', 'r').read())
    else:
        cache = {}
    if spatial_text in cache:
        return cache[spatial_text]
    else:

        # Don't send requests to geonames too fast.
        if (datetime.datetime.now() - time_of_last_geonames_request).total_seconds() < 0.5:
            time.sleep(0.5)
            time_of_last_geonames_request = datetime.datetime.now()

        url = geonames_baseurl + urllib.quote(spatial_text)
        fo = urllib.urlopen(url)
        res = fo.read()
        res = json.loads(res)
        if res['geonames']:
            result = {
                    'lat': res['geonames'][0]['lat'],
                    'lon': res['geonames'][0]['lng']
                    }
            cache[spatial_text] = result
            json.dump(cache, open('cache/geonames.json', 'w'))
            return result
        else:
            return None

if not os.path.exists('cache'):
    os.makedirs('cache')

if not os.path.exists('cache/datacatalogs.json'):
    url = 'http://datacatalogs.org/api/search/dataset?q=&limit=500&all_fields=1'
    urllib.urlretrieve(url, 'cache/datacatalogs.json')

num_geocoded = 0
num_failed = 0
rawtext = open('cache/datacatalogs.json', 'r').read()
datasets = json.loads(rawtext)
for dataset in datasets['results']:

    # Special-case some problem datasets.
    # FIXME: These may not all be correct.
    # TODO:
    # bouche-rhone-visitprovence
    special_cases = {
        'allerdale': 'Allerdale',
        'bordeaux_fr': 'Bordeaux',
        'dati-lombardia': 'Lombardia, Italy',
        'dnv_org': 'North Vancouver',
        'dublinked-datastore': 'Dublin',
        'gironde-aquitaine_fr': 'Gironde',
        'go-geo': 'United Kingdom',
        'montpellier_fr': 'Montpellier',
        'mosman-council-datastore': 'Mosman',
        'nantes_fr': 'Nantes',
        'new-orleans-louisiana': 'New-Orleans',
        'opendata-lv': 'Latvia',
        'openstreetmap': 'Earth',
        'portal-de-datos-abiertos-de-jccm': 'Castilla-La Mancha',
        'provincia-roma': 'Rome',
        'region-of-waterloo-ontario': 'Waterloo, Ontario',
        'rennes_fr': 'Rennes',
        'salford': 'Salford',
        'saone-et-loire_fr': 'Saone-et-Loire',
        'toulouse_fr': 'Toulouse',
        'us-department-of-labor-enforcement-data': 'USA',
        'victoria-australian-state-open-data-catalogue': 'Victoria, Australia',
        }
    if dataset['name'] in special_cases:
        dataset['extras']['spatial_text'] = special_cases[dataset['name']]

    spatial_text = dataset['extras']['spatial_text']
    spatial_text = spatial_text.encode('utf8', 'ignore')
    location = geonames_lookup(spatial_text)
    if location:
        dataset['location'] = location
        print "Geocoded dataset {name}".format(**dataset)
        num_geocoded = num_geocoded + 1
    else:
        print "No geonames in result for dataset {name}".format(**dataset)
        num_failed = num_failed + 1

    # Promote the dataset's extras to top-level keys.
    dataset['spatial_code'] = dataset['extras']['spatial']
    del dataset['extras']['spatial']
    dataset.update(dataset['extras'])
    del dataset['extras']

    # Delete any empty values.
    for key in dataset.keys():
        if not dataset[key]:
            del dataset[key]

json.dump(datasets['results'],
        open('cache/datacatalogs.geocoded.json', 'w'))
print "Geocoded {0} datasets, {1} failed".format(num_geocoded, num_failed)

# Use ckanclient (https://github.com/okfn/ckanclient) to upload the resource
# to thedatahub.org.
import ckanclient.datastore
datastore_url = 'http://datahub.io/api/data/39317285-d0e8-4dad-9e5d-f064100132c9'
client = ckanclient.datastore.DataStoreClient(datastore_url)

# Specify that the 'location' field is a geo_point.
mapping = {
    'properties': {
        'location': {
            'type': 'geo_point'
            }
    }
}

client.delete()
client.mapping_update(mapping)
client.upload('cache/datacatalogs.geocoded.json', refresh=True)
