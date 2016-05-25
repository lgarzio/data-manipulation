#! /usr/local/bin/python

import pandas as pd
import os

'''
This script combines all ingest csvs cloned in a local repo from https://github.com/ooi-integration/ingestion-csvs
'''

rootdir = '/Users/lgarzio/Documents/repo/ooi-integration/ingestion-csvs'

df = pd.DataFrame()
for root, dirs, files in os.walk(rootdir):
    for f in files:
        if f.endswith('ingest.csv'):
            with open(os.path.join(root,f),'r') as csv_file:
                filereader = pd.read_csv(csv_file)

                # add the file name as a column
                filereader['ingest_csv_filename'] = str(f)

                # split uframe_route to get driver name and add as a column
                dr = filereader['uframe_route'].str.split('.').str[1]
                dr = dr.str.replace('-', '_')
                filereader['driver'] = dr

                df = df.append(filereader)
                #df = df.rename(columns={'Unnamed: 4':'ingest_csv_notes'})

header = ['reference_designator','driver','uframe_route','data_source','filename_mask','ingest_csv_filename']
df.to_csv('/Users/lgarzio/Documents/OOI/ingestion_csvs_concat.csv',index = False, columns = header, na_rep = 'NaN')