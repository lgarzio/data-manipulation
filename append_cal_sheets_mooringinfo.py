#! /usr/local/bin/python

"""
Created on Tues Aug 23 2016

@author: lgarzio
"""

import pandas as pd
import os

'''
This script combines the first sheet (called 'Moorings') of archived OOI cal sheets cloned in a local repo from
https://github.com/ooi-integration/asset-management/tree/master/ARCHIVE/deployment

'''

rootdir = '/Users/lgarzio/path_to_local_repo'

df = pd.DataFrame()
for root, dirs, files in os.walk(rootdir):
    for f in files:
        if f.startswith('Omaha_Cal_Info'):
            with open(os.path.join(root,f),'r') as excel_file:
                mooring_info = pd.read_excel(excel_file,sheetname=0)
                mooring_info2 = mooring_info.ix[0,0:14]
                # add the file name as a column
                mooring_info2['filename'] = str(f)
                df = df.append(mooring_info2)

mooring_header = ['Mooring OOIBARCODE','Ref Des','Serial Number','Deployment Number','Anchor Launch Date',
                  'Anchor Launch Time','Recover Date','Latitude','Longitude','Water Depth',
                  'Cruise Number','Notes','Lat','Lon','filename']
df.to_csv('/Users/lgarzio/output_file.csv',index = False, columns = mooring_header, na_rep = 'NaN',encoding='utf-8')