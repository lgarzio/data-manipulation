
"""
Created on 1/4/2018

@author: lgarzio
@brief: This script is used to compare the reference designators and streams in the QC database to uFrame and the
GUI data catalog list.
@usage:
username: username to access the OOI API
token: password to access the OOI API
out: location to save output
"""

import requests
import os
import datetime as dt
import pandas as pd


def define_source(df):
    df['source'] = 'x'
    df['source'][(df['in_qcdb']=='yes') & (df['in_uframe']=='yes') & (df['in_gui_catalog']=='yes')] = 'in_all_3'
    df['source'][(df['in_qcdb']=='yes') & (df['in_uframe']=='yes') & (df['in_gui_catalog'].isnull())] = 'not_in_gui_catalog'
    df['source'][(df['in_qcdb']=='yes') & (df['in_uframe'].isnull()) & (df['in_gui_catalog']=='yes')] = 'not_in_uframe'
    df['source'][(df['in_qcdb'].isnull()) & (df['in_uframe']=='yes') & (df['in_gui_catalog']=='yes')] = 'not_in_qcdb'
    df['source'][(df['in_qcdb']=='yes') & (df['in_uframe'].isnull()) & (df['in_gui_catalog'].isnull())] = 'in_qcdb_only'
    df['source'][(df['in_qcdb'].isnull()) & (df['in_uframe']=='yes') & (df['in_gui_catalog'].isnull())] = 'in_uframe_only'
    df['source'][(df['in_qcdb'].isnull()) & (df['in_uframe'].isnull()) & (df['in_gui_catalog']=='yes')] = 'in_gui_catalog_only'
    return df


def get_qc_database():
    db_inst_stream = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/data_streams.csv')
    db_stream_desc = pd.read_csv('https://raw.githubusercontent.com/seagrinch/data-team-python/master/infrastructure/stream_descriptions.csv')

    db_inst_stream = db_inst_stream[['reference_designator','method','stream_name']]
    db_stream_desc = db_stream_desc.rename(columns={'name':'stream_name'})
    db_stream_desc = db_stream_desc[['stream_name','stream_type']]
    db = pd.merge(db_inst_stream,db_stream_desc,on='stream_name',how='outer')
    db['in_qcdb'] = 'yes'
    db = db[db.reference_designator.notnull()]
    db['refdes_method_stream'] = db['refdes_method_stream'] = db['reference_designator'] + '__' + db['method'] + '__' + db['stream_name']

    return db


def get_gui_catalog_database():
    r = requests.get('https://ooinet.oceanobservatories.org/api/uframe/stream')
    response = r.json()['streams']

    gui_catalog = []
    for i in range(len(response)):
        try:
            method = response[i]['stream_method'].replace('-','_')
        except AttributeError:  # skip if there is no method defined
            method = 'no_method'

        if not response[i]['stream']:
            stream = 'no_stream'
        else:
            stream = response[i]['stream']

        refdes = response[i]['reference_designator']
        refdes_method_stream = refdes + '__' + method + '__' + stream
        gui_catalog.append(refdes_method_stream)
    gui_catalog_db = pd.DataFrame(gui_catalog,columns=['refdes_method_stream'])
    gui_catalog_db['in_gui_catalog'] = 'yes'
    gui_catalog_db['reference_designator'] = gui_catalog_db['refdes_method_stream'].str.split('__').str[0]
    gui_catalog_db['method'] = gui_catalog_db['refdes_method_stream'].str.split('__').str[1]
    gui_catalog_db['stream_name'] = gui_catalog_db['refdes_method_stream'].str.split('__').str[2]
    return gui_catalog_db


def get_uframe_response(session, url, username, token):
    r = session.get(url, auth=(username, token))
    response = r.json()
    return response


def get_uframe_database(username, token):
    url = 'https://ooinet.oceanobservatories.org/api/m2m/12576/sensor/inv'
    session = requests.session() # open the connection and leave it open for the session

    platforms = get_uframe_response(session, url, username, token)

    uframe_list=[]
    for p in platforms:
        #if p == 'GA03FLMA':
        url2 = url + '/' + p
        nodes = get_uframe_response(session, url2, username, token)
        for n in nodes:
            url3 = url2 + '/' + n
            insts = get_uframe_response(session, url3, username, token)
            for i in insts:
                url4 = url3 + '/' + i
                methods = get_uframe_response(session, url4, username, token)
                for m in methods:
                    url5 = url4 + '/' + m
                    streams = get_uframe_response(session, url5, username, token)
                    for s in streams:
                        refdes_method_stream = p + '-' + n + '-' + i + '__' + m + '__' + s
                        uframe_list.append(refdes_method_stream)

    uframe_db = pd.DataFrame(uframe_list,columns=['refdes_method_stream'])
    uframe_db['in_uframe'] = 'yes'
    uframe_db['reference_designator'] = uframe_db['refdes_method_stream'].str.split('__').str[0]
    uframe_db['method'] = uframe_db['refdes_method_stream'].str.split('__').str[1]
    uframe_db['stream_name'] = uframe_db['refdes_method_stream'].str.split('__').str[2]
    return uframe_db


def main(username, token, out):
    now = dt.datetime.now().strftime('%Y%m%dT%H%M')
    fname = 'compare_qcdb_uframe_gui_%s.csv' %now
    qcdb = get_qc_database()
    qcdb.to_csv(os.path.join(out, 'test_qcdb.csv'), index=False)

    uframe_db = get_uframe_database(username, token)
    uframe_db.to_csv(os.path.join(out, 'test_uframe.csv'), index=False)

    gui_catalog_db = get_gui_catalog_database()
    gui_catalog_db.to_csv(os.path.join(out, 'test_gui_catalog.csv'), index=False)

    merge_on = ['refdes_method_stream','reference_designator','method','stream_name']

    compare = qcdb.merge(uframe_db,on=merge_on,how='outer').merge(gui_catalog_db,on=merge_on,how='outer')
    compare_df = define_source(compare)
    compare_df.sort_values(by='refdes_method_stream').to_csv(os.path.join(out, fname), index=False)


if __name__ == '__main__':
    username = 'api_username'
    token = 'api_token'
    out = '/Users/lgarzio/Documents/OOI/'
    main(username, token, out)