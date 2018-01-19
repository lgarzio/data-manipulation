#!/usr/bin/env python
"""
Created on Jan 19 2018

@author: lgarzio

@brief: This script is used create new ingest csvs for a specific instrument in a platform directory from
https://github.com/ooi-integration/ingestion-csvs. Useful when needing to purge and re-ingest all deployments of a
specific instrument for a platform.
"""

import os
import pandas as pd

rootdir = '/Users/lgarzio/Documents/repo/lgarzio/ooi-integration-fork/ingestion-csvs/CP02PMUO'
inst = 'FLORT'

for root, dirs, files in os.walk(rootdir):
    for filename in files:
        if filename.endswith('_ingest.csv'):
            print filename
            file = pd.read_csv(os.path.join(rootdir,filename))
            f = file.loc[file.reference_designator.str.contains(inst)]
            fname = filename[0:-4] + '_' + inst + '.csv'
            f.to_csv(os.path.join(rootdir,fname),index=False)