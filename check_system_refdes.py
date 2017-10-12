
"""
Created on Oct 10 2017

@author: lgarzio
@brief: This script is used to compare the master list of reference designators in the vocab.csv to the reference
designators ingested in uFrame.
@usage:
AM_vocab: location of the asset management vocab.csv cloned on the local machine from https://github.com/ooi-integration/asset-management
username: username to access the OOI API
token: password to access the OOI API
saveDir: location to save output
"""

import requests
import os
from datetime import datetime
import pandas as pd

AM_vocab = '/Users/lgarzio/Documents/repo/lgarzio/ooi-integration-fork/asset-management/vocab/vocab.csv'
username = 'enter_username'
token = 'enter_password'
saveDir = '/Users/lgarzio/Documents/OOI/'

url = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv/'

# Import vocab.csv
vocab = pd.read_csv(AM_vocab)
vocab_refdes = vocab['Reference_Designator']
vocab_refdes = vocab_refdes[vocab_refdes.str.len()==27] # remove non-instruments

# Get list of refdes from uframe
session = requests.session() # open the connection and leave it open for the session

platforms = session.get(url, auth=(username, token))
dfp = pd.read_json(platforms.text)

uframe_list=[]
for x,xx in dfp.iterrows():
    url2 = url + xx[0]
    nodes = session.get(url2, auth=(username, token))
    dfn = pd.read_json(nodes.text)
    for y,yy in dfn.iterrows():
        url3 = url2 + '/' + yy[0]
        inst = session.get(url3, auth=(username, token))
        dfi = pd.read_json(inst.text)
        for z,zz in dfi.iterrows():
            refdes = xx[0] + '-' + yy[0] + '-' + zz[0]
            uframe_list.append(refdes)

filename = 'uframe_refdes_%s.csv' % datetime.now().strftime('%Y%m%d')
fname = os.path.join(saveDir,filename)
pd.DataFrame(uframe_list,columns=['refdes']).to_csv(fname,index=False)

# Compare lists
s_vocab_refdes = set(vocab_refdes)
s_uframe_list = set(uframe_list)

both = pd.DataFrame(list(s_vocab_refdes & s_uframe_list))  # list of refdes in the vocab.csv and uframe
only_vocab = pd.DataFrame(list(s_vocab_refdes - s_uframe_list)) # list of refdes only in the vocab.csv
only_uframe = pd.DataFrame(list(s_uframe_list - s_vocab_refdes)) # list of refdes only in uframe

# Add column names to dataframes
both.columns = ['refdes']
only_vocab.columns = ['refdes']
only_uframe.columns = ['refdes']

# Output files
outfile1 = os.path.join(saveDir,'check_system_refdes_inboth.csv')
outfile2 = os.path.join(saveDir,'check_system_refdes_onlyvocab.csv')
outfile3 = os.path.join(saveDir,'check_system_refdes_onlyuframe.csv')

# Export to csv, sorted alphabetically
both.sort_values(by='refdes').to_csv(outfile1, index=False)
only_vocab.sort_values(by='refdes').to_csv(outfile2, index=False)
only_uframe.sort_values(by='refdes').to_csv(outfile3, index=False)