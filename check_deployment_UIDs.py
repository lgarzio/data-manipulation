#! /usr/local/bin/python

"""
Created on Mon Jan 23 2017

@author: lgarzio
"""

import pandas as pd
import os

'''
This script loops through all deployment csvs cloned in a local repo from https://github.com/ooi-integration/asset-management
and lists where the Reference Designator doesn't match the sensor.uid
'''

rootdir = '/Users/lgarzio/path_to_local_repo'
sensor_bulk_load = '/Users/lgarzio/path_to_local_repo/asset-management/bulk/sensor_bulk_load-AssetRecord.csv'

df = pd.DataFrame()
for root, dirs, files in os.walk(rootdir):
    for f in files:
        if f.endswith('Deploy.csv'):
            with open(os.path.join(root,f),'r') as csv_file:
                filereader = pd.read_csv(csv_file)

                # add just the instrument name from Reference Designator and sensor.uid as columns to data frame
                filereader['refdes_inst'] = filereader['Reference Designator'].str.split('-').str[3].str[0:6]
                filereader['sensor.uid_inst'] = filereader['sensor.uid'].str.split('-').str[1]

                # add the file name as a column
                filereader['filename'] = str(f)

                # append all deployment sheets in one file
                df = df.append(filereader)
                df['refdes_equals_uid'] = df['refdes_inst'] == df['sensor.uid_inst']

                # merge with sensor_bulk_load
                sbl = pd.read_csv(sensor_bulk_load)
                sbl2 = sbl.rename(columns = {'ASSET_UID':'sensor.uid'})
                df_bulk = pd.merge(df, sbl2, on='sensor.uid', how='left')
                df_bulk2 = df_bulk.rename(columns = {'DESCRIPTION OF EQUIPMENT':'DESCRIPTION OF EQUIPMENT (sensor_bulk_load)',
                                                                     'Manufacturer':'Manufacturer (sensor_bulk_load)',
                                                                     'Model':'Model (sensor_bulk_load)'})


                header = ['filename','CUID_Deploy','Reference Designator','deploymentNumber','startDateTime','stopDateTime','mooring.uid','node.uid',
                          'sensor.uid','lat','lon','deployment_depth','water_depth','notes','refdes_inst','sensor.uid_inst','refdes_equals_uid',
                          'DESCRIPTION OF EQUIPMENT (sensor_bulk_load)','Manufacturer (sensor_bulk_load)','Model (sensor_bulk_load)']

df_bulk2.to_csv('/Users/lgarzio/output_file.csv', index = False, columns = header)