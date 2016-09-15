# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

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

    for skim_dir in ['base-data', 'build-data']:

        csv_dir = os.path.join(base_dir, skim_dir)
        omx_dir = csv_dir

        cvt_to_omx(os.path.join(csv_dir, 'freight_trips.csv'),
                   os.path.join(omx_dir, 'testtrips.omx'), 'freight_trips')

        cvt_to_omx(os.path.join(csv_dir, 'freight_ivt.csv'),
                   os.path.join(omx_dir, 'testlos.omx'), 'freight_ivt')

        cvt_to_omx(os.path.join(csv_dir, 'freight_opercost.csv'),
                   os.path.join(omx_dir, 'testlos.omx'), 'freight_opercost')

        cvt_to_omx(os.path.join(csv_dir, 'freight_tollcost.csv'),
                   os.path.join(omx_dir, 'testlos.omx'), 'freight_tollcost')

cvt_freight_skims()

#################

# base_dir = os.path.join(os.path.dirname(__file__), '..', 'bca4abm', 'tests', 'data')
#
# for skim_dir in ['base-data', 'build-data']:

# omx_file_name = os.path.join(data_dir, skim_dir, 'skim.omx')
# omx_file = omx.openFile(omx_file_name, 'r')
#
# print "----- ivt"
# m = omx_file['ivt']
# print type(m)
# print "atom: %s" % m.atom
# print "shape", m.shape
# print "array type", type( m[:,:])
# print m[:,:]
#
# print "----- cvt"
# m2 = omx_file['cvt']
# print type(m2)
# print "atom: %s" % m2.atom
# print "shape", m2.shape
# print "array type", type( m2[:,:])
# print m2[:,:]
#
# print "----- sum"
# print m[:,:] + m2[:,:]
#
# omx_file.close()

