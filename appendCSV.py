#! /usr/local/bin/python

import os

'''
This script combines all ingest csvs cloned in a local repo from https://github.com/ooi-integration/ingestion-csvs
'''

def appendCSV(rootDir,output_file):
    with open(output_file,'w') as outfile:
        header = ''
        for root,dirs,files in os.walk(rootDir):
            for f in files:
                if f.endswith('ingest.csv'):
                    with open(os.path.join(root,f),'r') as csvfile:
                        if header=='':
                            header=csvfile.readline()
                            outfile.write(header)
                        else:
                            csvfile.readline()
                        for line in csvfile:
                            outfile.write(line)

rootDir = '/Users/lgarzio/Documents/repo/ooi-integration/ingestion-csvs'
output_file = '/Users/lgarzio/Documents/OOI/ingestion_csvs_concat.csv'
#output_file = os.path.join(rootDir,'test.csv')

appendCSV(rootDir,output_file)