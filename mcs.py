'''
Mapillary Coverage Scraper (MCS)
Created on 2020-03-06 by John M Kelly

Queries Mapillary API for drives (sequences) 15 months prior to date and joins with
RWT driveroutes

version 1.0

'''

import datetime
import json
import os
import requests
try:
    import pandas as pd
except:
    print('pandas not found, please install first')
try:
    import geopandas as gpd
except:
    print('geopandas not found, please install first')


startTime = datetime.datetime.now()

#get test code and search the driveroutes
test_code = input('Enter test code: ')

shp_list = []

print('Searching driveroutes')
for ps, fs, fns in os.walk(r'\\smb-atl1isn03.ad.here.com\RWT_QUEST\RWT\2020\Non_RWT_Drive'):
    for fn in fns:
        if fn.endswith('_driveroute.shp'):
            shp_list.append(os.path.join(ps, fn))

for item in shp_list:
    if item.endswith(test_code + '_driveroute.shp'):
        print('Found driveroute on server')
        driveroute = item       
        dr_path = os.path.split(driveroute)[0]

# auth info
username = 'HERERWT'
client_id = 'NHEwZ1JocWJSQWFpcmI0VURRQV9KUToxMzE1M2M4ZDZmY2I5MmQ1'
client_secret = 'NWJiZWU5ZWYxMGNlZGFlMmUwNThiMGRhOWM0MDFmNWM='

# search dates.  string format = yyyy-mm-dd
start_dt = datetime.datetime.today() - pd.DateOffset(months=15) # create date 15 months prior
start = start_dt.strftime('%Y-%m-%d')
today = datetime.datetime.today().strftime('%Y-%m-%d')


print('\n                 YYYY-MM-DD\nUsing start date {}\nUsing end date   {}'.format(start, today))

# endpoint
mapillary = 'https://a.mapillary.com/v3/sequences'


def mapfunc(b):
    
    output = {"type":"FeatureCollection","features":[]}
    params = {'client_id' : client_id,
              'bbox' : b,
              'start_time' : start,
              'end_time' : today,
              'per_page' : 1000 #responses per page, 1000 is max

              }

    # call api
    print('Calling API \n ')
    response = requests.get(mapillary,params=params)
    while response.status_code != 200:
            print('Status code = {} RETRYING...'.format(response.status_code))
            response = requests.get(mapillary,params=params, timeout=None)
    if response.status_code == 200:
        print('Status code = {} OK!'.format(response.status_code))
    else:
        print('Status code = {} Something wrong!'.format(response.status_code))
    
    num = 2
    data = response.json()
    
    data_length = len(data['features'])
    print('Number of features in first set: {}'.format(data_length))
    
    # append response to output data
    for d in data['features']:
        output['features'].append(d)
        
    # loop through each "page" of results
    while data_length == 1000:
        
        print('Pass number {}'.format(num))
        link = response.links['next']['url'] # this is the next page of results
        response = requests.get(link)
        while response.status_code != 200:
            print('Status code = {} RETRYING...'.format(response.status_code))
            response = requests.get(link, timeout=None)
        data = response.json()
        for f in data['features']:
            output['features'].append(f)
        print("Total features: {}".format(len(output['features'])))
        data_length = len(data['features'])
        num += 1
    
    #save data to geojson file
    with open(os.path.join(dr_path,test_code + '_mapillary.geojson'), 'w') as outfile:
        print('\nWriting to geojson file')
        json.dump(output, outfile)
        print('DONE')
    
    #save data to shpfile
    print('\nSaving to shpfile')
    gdf_m = gpd.read_file(os.path.join(dr_path,test_code + '_mapillary.geojson'))
    
    # do datetime stuff
    gdf_m['date64'] = gdf_m['captured_at'].astype('datetime64')
    gdf_m['year_month'] = gdf_m['date64'].map(lambda x: x.strftime('%Y-%b'))
    
    #buffer mapillary file
    gdf_m.buffer(0.00025, resolution = 24) 
    
    #spatial join
    gdf_sjoin = gpd.sjoin(gdf, gdf_m, how = 'left', op = 'intersects') #this creates duplicates!
    
    #save to shpfile
    gdf_sjoin.drop('date64', axis=1, inplace=True)
    gdf_sjoin.to_file(os.path.join(dr_path,test_code + '_mapillary.shp'))
    
    print('DONE')
    
    print('\nTime elapsed: {}'.format(datetime.datetime.now() - startTime))


# get the bounding box of drivefile and set to a bounding box string variable 'b'
gdf = gpd.read_file(driveroute)
minx, miny, maxx, maxy = gdf.total_bounds
b = '{},{},{},{}'.format(minx, miny, maxx, maxy)
mapfunc(b)