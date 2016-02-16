import orca
import pandas as pd
import numpy as np
import os

# there is something a little bit too opaque about this:
# the following import has the side-effect of registering injectables
from bca4abm import bca4abm as bca

#orca.add_injectable("configs_dir", os.path.dirname(__file__))
#orca.add_injectable("data_dir", os.path.dirname(__file__))


settings = orca.eval_variable('settings')
if 'store' not in settings:
    orca.add_injectable("store", None)

orca.run(["demographics_processor"])
orca.run(["person_trips_processor"])

#orca.run(["write_output_store"])

bca_base_trips_with_demographics = \
    orca.eval_variable('bca_base_trips_with_demographics').to_frame()

print bca_base_trips_with_demographics[
    ["coc_age", "coc_poverty", "travel_time_benefit", "vot", "monetized_travel_time_benefit"]]

# aggregations = {
#     'travel_time_benefit':'sum',
# }
#
# grouped = bca_base_trips_with_demographics.groupby(['tour_purpose_cat', 'coc_age', 'coc_poverty'])
# print grouped.agg(aggregations)
