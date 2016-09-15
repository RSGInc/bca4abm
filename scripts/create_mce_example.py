# ActivitySim
# See full license in LICENSE.txt.

import os

import numpy as np
import pandas as pd
import openmatrix as omx
import itertools


MAX_ZONE = 25
SOURCE_BASE_DIR = '2040NB_Base'
SOURCE_BUILD_DIR = '2040NB_Build'

CANONICAL_BASE_DIR = 'base-data'
CANONICAL_BUILD_DIR = 'build-data'

source_data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'metro_mce_data')
dest_data_dir = os.path.join(os.path.dirname(__file__), '..', 'example_4step', 'data')


def slice_taz_file(source_data_dir, dest_data_dir, file_name, max_zone):
    in_file_path = os.path.join(source_data_dir, file_name)
    out_file_path = os.path.join(dest_data_dir, file_name)
    cval = pd.read_csv(in_file_path)
    cval = cval.head(MAX_ZONE)
    cval.to_csv(out_file_path, index=False, header=True)
    print "copy_taz %s: %s rows, %s columns" % (file_name, cval.shape[0], cval.shape[1])


def slice_links_file(source_data_dir, dest_data_dir, file_name, max_zone):
    in_file_path = os.path.join(source_data_dir, file_name)
    out_file_path = os.path.join(dest_data_dir, file_name)
    cval = pd.read_csv(in_file_path)

    cval = cval[(cval['@zone']<=max_zone)]

    cval.to_csv(out_file_path, index=False, header=True)

    print "copy_links %s: %s rows, %s columns" % (file_name, cval.shape[0], cval.shape[1])


def slice_skims(source_data_dir, dest_data_dir, file_name, max_zone):

    in_file_path = os.path.join(source_data_dir, file_name)
    out_file_path = os.path.join(dest_data_dir, file_name)

    # process all skims
    skims = omx.openFile(in_file_path)
    skims_out = omx.openFile(out_file_path, 'a')

    skimsToProcess = skims.listMatrices()
    for skimName in skimsToProcess:
        print "slice_skims %s: %s" % (file_name, skimName)
        skims_out[skimName] = skims[skimName][0:max_zone, 0:max_zone]
        skims_out[skimName].attrs.TITLE = ''  # remove funny character for OMX viewer


for file_name in ['mf.cval.csv',
                  'ma.hbomdcls.csv',
                  'ma.hbsldcls.csv',
                  'ma.nhbnwdcls.csv',
                  'ma.hbrhdcls.csv',
                  'ma.hbsmdcls.csv',
                  'ma.nhbwdcls.csv',
                  'ma.hbcdcls.csv',
                  'ma.hbrldcls.csv',
                  'ma.hbwhdcls.csv',
                  'ma.schdcls.csv',
                  'ma.hbohdcls.csv',
                  'ma.hbrmdcls.csv',
                  'ma.hbwldcls.csv'
                  ]:
    slice_taz_file(os.path.join(source_data_dir, SOURCE_BASE_DIR),
               os.path.join(dest_data_dir, CANONICAL_BASE_DIR),
               file_name,
               MAX_ZONE)

for file_name in ['base_linksMD1.csv', 'base_linksPM2.csv']:
    slice_links_file(os.path.join(source_data_dir, SOURCE_BASE_DIR),
               os.path.join(dest_data_dir, CANONICAL_BASE_DIR),
               file_name,
               MAX_ZONE)


for file_name in ['skims_mfs.omx', 'assign_mfs.omx']:
    slice_skims(os.path.join(source_data_dir, SOURCE_BASE_DIR),
               os.path.join(dest_data_dir, CANONICAL_BASE_DIR),
               file_name,
               MAX_ZONE)
