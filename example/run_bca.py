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

orca.add_injectable('configs_dir', os.path.join(parent_dir, 'configs'))
orca.add_injectable('data_dir', os.path.join(parent_dir, 'data'))

parent_dir = os.path.dirname(__file__)
orca.add_injectable('output_dir', os.path.join(parent_dir, 'output'))

orca.run(['initialize_output_store'])
with orca.eval_variable('output_store_for_read') as hdf:
    assert hdf.keys() == []

orca.run(['demographics_processor'])
orca.run(['person_trips_processor'])
orca.run(['aggregate_trips_processor'])

# trips_df = orca.get_table('trips_with_demographics').to_frame().sort(['index1'])
# trips_df.to_csv(os.path.join(parent_dir, 'output', 'trips_with_demographics.csv') )

# print "### FINAL monetized_travel_time_benefit", trips_df.monetized_travel_time_benefit.sum()
# print "### FINAL travel_time_benefit", trips_df.travel_time_benefit.sum()

with orca.eval_variable('output_store_for_read') as hdf:

    for key in hdf.keys():
        print "\n###\n### %s\n###\n%s\n" % (key, hdf[key])


