import orca
import pandas as pd
import numpy as np
import os

import openmatrix as omx

#################

def omx_setMatrix(omx_file, omx_key, data):

    # omx_file.__setitem__ this throws a NodeError if omx_key already exists
    if omx_key in omx_file.listMatrices():
        #print "deleting existing key", omx_key
        omx_file.removeNode(omx_file.root.data, omx_key)

    omx_file[omx_key] = data

def cvt_to_omx(txt_file_name, omx_file_name, omx_key):

    data = np.loadtxt(txt_file_name, dtype=np.float, delimiter=',')

    print "---"
    print "cvt_to_omx from", txt_file_name
    print "cvt_to_omx   to %s / %s" % (omx_file_name, omx_key)
    # print m

    omx_file = omx.openFile(omx_file_name, 'a')
    omx_setMatrix(omx_file, omx_key, data)
    omx_file.close()

def cvt_freight_skims():

    base_dir = os.path.join(os.path.dirname(__file__), '..', 'bca4abm', 'tests', 'data')

    for skim_dir in ['base-matrices', 'build-matrices']:

        csv_dir = os.path.join(base_dir, skim_dir)
        omx_dir = csv_dir

        cvt_to_omx(os.path.join(csv_dir, 'freight_trips.csv'),
                   os.path.join(omx_dir, 'trips.omx'), 'non_toll')

        cvt_to_omx(os.path.join(csv_dir, 'freight_trips.csv'),
                   os.path.join(omx_dir, 'trips.omx'), 'toll')

        cvt_to_omx(os.path.join(csv_dir, 'freight_ivt.csv'),
                   os.path.join(omx_dir, 'skim.omx'), 'ivt')

        cvt_to_omx(os.path.join(csv_dir, 'freight_aoc.csv'),
                   os.path.join(omx_dir, 'skim.omx'), 'aoc')

        cvt_to_omx(os.path.join(csv_dir, 'freight_cvt.csv'),
                   os.path.join(omx_dir, 'skim.omx'), 'cvt')

cvt_freight_skims()
