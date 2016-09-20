# ActivitySim
# See full license in LICENSE.txt.

import os

import numpy as np
import pandas as pd
import openmatrix as omx
import itertools




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
    skims_out = omx.openFile(out_file_path, 'w')

    skimsToProcess = skims.listMatrices()
    for skimName in skimsToProcess:
        print "slice_skims %s: %s" % (file_name, skimName)
        skims_out[skimName] = skims[skimName][0:max_zone, 0:max_zone]
        skims_out[skimName].attrs.TITLE = ''  # remove funny character for OMX viewer


def copy_slice(source_dir, dest_dir,
               taz_file_names, skim_file_names, link_file_names):

    for file_name in taz_file_names:
        slice_taz_file(source_dir, dest_dir, file_name, MAX_ZONE)

    for file_name in skim_file_names:
        slice_skims(source_dir, dest_dir, file_name, MAX_ZONE)

    for file_name in link_file_names:
        slice_links_file(source_dir, dest_dir, file_name, MAX_ZONE)


TAZ_FILES = [
    'mf.cval.csv',
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
    ]

SKIM_FILES = [
    'skims_mfs.omx',
    'assign_mfs.omx'
]

MAX_ZONE = 25
SOURCE_BASE_DIR = '2040NB_Base'
SOURCE_BUILD_DIR = '2040NB_Build'

SOURCE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'metro_mce_data')
DEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'example_4step', 'data')


copy_slice(
    source_dir=os.path.join(SOURCE_DATA_DIR, '2040NB_Base'),
    dest_dir=os.path.join(DEST_DATA_DIR, 'base-data'),
    taz_file_names=TAZ_FILES,
    skim_file_names=SKIM_FILES,
    link_file_names=['base_linksMD1.csv', 'base_linksPM2.csv']
)

copy_slice(
    source_dir=os.path.join(SOURCE_DATA_DIR, '2040NB_Build'),
    dest_dir=os.path.join(DEST_DATA_DIR, 'build-data'),
    taz_file_names=TAZ_FILES,
    skim_file_names=SKIM_FILES,
    link_file_names=['build_linksMD1.csv', 'build_linksPM2.csv']
)
