import urllib
url = 'http://datacatalogs.org/api/search/dataset?q=&limit=500&all_fields=1'
urllib.urlretrieve(url, 'data/datacatalogs.json')

