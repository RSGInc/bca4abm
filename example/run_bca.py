import orca
import pandas as pd
import numpy as np
import os

# there is something a little bit too opaque about this:
# the following import has the side-effect of registering injectables
from bca4abm import bca4abm as bca


parent_dir = os.path.join(os.path.dirname(__file__), '..', 'bca4abm', 'tests')
#parent_dir = os.path.dirname(__file__)

orca.add_injectable('configs_dir', os.path.join(parent_dir, 'configs'))
orca.add_injectable('data_dir', os.path.join(parent_dir, 'data'))

parent_dir = os.path.dirname(__file__)
orca.add_injectable('output_dir', os.path.join(parent_dir, 'output'))

settings = orca.eval_variable('settings')
if 'store' not in settings:
    orca.add_injectable('store', None)

orca.run(['demographics_processor'])
orca.run(['person_trips_processor'])

#orca.run(['write_output_store'])

bca_trips_with_demographics = \
    orca.eval_variable('bca_trips_with_demographics').to_frame()

print bca_trips_with_demographics[
    ['travel_time_benefit', 'vot', 'monetized_travel_time_benefit']]

print bca_trips_with_demographics[
    ['build', 'travel_time', 'travel_time_alt', 'travel_time_benefit']]

# aggregations = {
#     'travel_time_benefit':'sum',
# }
#
# grouped = bca_base_trips_with_demographics.groupby(['tour_purpose_cat', 'coc_age', 'coc_poverty'])
# print grouped.agg(aggregations)
