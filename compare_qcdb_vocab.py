#!/usr/bin/env python
"""
Created on Oct 23 2017
@author: lgarzio
@brief: This script is used to compare the master list of reference designators in the asset management vocab.csv to 
the reference designators in the data team's QC database.
@usage:
saveDir: location to save output
"""

import pandas as pd
import os

saveDir = '/Users/lgarzio/Documents/OOI/'

vocab_url = 'https://raw.githubusercontent.com/ooi-integration/asset-management/master/vocab/vocab.csv'
db_url = 'https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/instruments.csv'

vocab_csv = pd.read_csv(vocab_url,index_col=None)
vocab_refdes = vocab_csv['Reference_Designator']
vocab_refdes = vocab_refdes[vocab_refdes.str.len()==27] # remove non-instruments

db_csv = pd.read_csv(db_url,index_col=None)
db_refdes = db_csv['reference_designator']
db_refdes = db_refdes[db_refdes.str.len()==27] # remove non-instruments

# Compare lists
s_vocab_refdes = set(vocab_refdes)
s_db_refdes = set(db_refdes)

both = pd.DataFrame(list(s_vocab_refdes & s_db_refdes))  # list of refdes in the vocab.csv and QC database
only_vocab = pd.DataFrame(list(s_vocab_refdes - s_db_refdes)) # list of refdes only in the vocab.csv
only_qcdb = pd.DataFrame(list(s_db_refdes - s_vocab_refdes)) # list of refdes only in QC database

both.columns = ['refdes'] # Add column name to dataframes
outfile1 = os.path.join(saveDir,'compare_refdes_inboth.csv')
both.sort_values(by='refdes').to_csv(outfile1, index=False) # Export to csv, sorted alphabetically

try:
    only_vocab.columns = ['refdes']
    outfile2 = os.path.join(saveDir,'compare_refdes_onlyvocab.csv')
    only_vocab.sort_values(by='refdes').to_csv(outfile2, index=False)
except ValueError: # if the df is empty
    print 'No refdes exist in the vocab.csv that are not in the QC database'

try:
    only_qcdb.columns = ['refdes']
    outfile3 = os.path.join(saveDir,'compare_refdes_onlyqcdb.csv')
    only_qcdb.sort_values(by='refdes').to_csv(outfile3, index=False)
except ValueError: # if the df is empty
    print 'No refdes exist in the QC database that are not in the vocab.csv'