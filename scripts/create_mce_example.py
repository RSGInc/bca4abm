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
    skims = omx.open_file(in_file_path)
    skims_out = omx.open_file(out_file_path, 'w')

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
    'ma.collpr.csv',
    'ma.hbcdcls.csv',
    'ma.hbohdcls.csv',
    'ma.hboldcls.csv',
    'ma.hbomdcls.csv',
    'ma.hboprh.csv',
    'ma.hboprl.csv',
    'ma.hboprm.csv',
    'ma.hbrhdcls.csv',
    'ma.hbrldcls.csv',
    'ma.hbrmdcls.csv',
    'ma.hbrprh.csv',
    'ma.hbrprl.csv',
    'ma.hbrprm.csv',
    'ma.hbshdcls.csv',
    'ma.hbsldcls.csv',
    'ma.hbsmdcls.csv',
    'ma.hbsprh.csv',
    'ma.hbsprl.csv',
    'ma.hbsprm.csv',
    'ma.hbwhdcls.csv',
    'ma.hbwldcls.csv',
    'ma.hbwmdcls.csv',
    'ma.hbwprh.csv',
    'ma.hbwprl.csv',
    'ma.hbwprm.csv',
    'ma.nhbnwdcls.csv',
    'ma.nhbwdcls.csv',
    'ma.nhnwpr.csv',
    'ma.nhwpr.csv',
    'ma.schdcls.csv',
    'ma.schpr.csv',
    'mf.cval.csv'
    ]

SKIM_FILES = [
    'skims_mfs.omx',
    'assign_mfs.omx'
]

MAX_ZONE = 25

SOURCE_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'metro_mce_data')
DEST_DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'example_4step', 'data')


copy_slice(
    source_dir=os.path.join(SOURCE_DATA_DIR, 'build-data'),
    dest_dir=os.path.join(DEST_DATA_DIR, 'build-data'),
    taz_file_names=TAZ_FILES,
    skim_file_names=SKIM_FILES,
    link_file_names=['linksMD1.csv', 'linksPM2.csv']
)

# python ./scripts/create_mce_example.py
