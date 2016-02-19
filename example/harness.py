import orca
import pandas as pd
import numpy as np
import os

# there is something a little bit too opaque about this:
# the following import has the side-effect of registering injectables
from bca4abm import bca4abm as bca

from bca4abm.util.misc import get_setting


parent_dir = os.path.join(os.path.dirname(__file__), '..', 'bca4abm', 'tests')
#parent_dir = os.path.dirname(__file__)

data_dir = os.path.join(parent_dir, 'data')
configs_dir = os.path.join(parent_dir, 'configs')

parent_dir = os.path.dirname(__file__)
output_dir = os.path.join(parent_dir, 'output')

orca.add_injectable('configs_dir', configs_dir)
orca.add_injectable('data_dir', data_dir)
orca.add_injectable('output_dir', output_dir)


import openmatrix as omx


#################

omx_file_name = os.path.join(data_dir, 'base-matrices', 'skim.omx')
omx_file = omx.openFile(omx_file_name, 'r')

print "----- ivt"
m = omx_file['ivt']
print type(m)
print "atom: %s" % m.atom
print "shape", m.shape
print "array type", type( m[:,:])
print m[:,:]

print "----- cvt"
m2 = omx_file['cvt']
print type(m2)
print "atom: %s" % m2.atom
print "shape", m2.shape
print "array type", type( m2[:,:])
print m2[:,:]

print "----- sum"
print m[:,:] + m2[:,:]


# sk = skim.Skim(m, offset=-1)
#
# omx_file.close()
#
# orig = [5, 9, 1]
# dest = [2, 9, 6]
#
#
# print "get:", sk.get(orig, dest)

omx_file.close()



#
# agg_trip_omx_file = omx.openFile(os.path.join(data_dir, "skim.omx"))
#
#
# skim_matrix = agg_trip_omx_file['DIST']
#
# distance_skim = skim.Skim(skim_matrix, offset=-1)
#
#
# skims = skim.Skims()
# skims['DISTANCE'] = orca.get_injectable("distance_skim")

