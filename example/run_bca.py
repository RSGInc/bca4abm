# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import orca
import pandas as pd
import numpy as np
import os

# there is something a little bit too opaque about this:
# the following import has the side-effect of registering injectables
from bca4abm import bca4abm as bca

parent_dir = os.path.dirname(__file__)

orca.add_injectable('configs_dir', os.path.join(parent_dir, 'configs'))
orca.add_injectable('data_dir', os.path.join(parent_dir, 'data'))
orca.add_injectable('output_dir', os.path.join(parent_dir, 'output'))

orca.run(['initialize_output_store'])

orca.run(['demographics_processor'])
orca.run(['person_trips_processor'])
orca.run(['auto_ownership_processor'])
orca.run(['physical_activity_processor'])
orca.run(['aggregate_trips_processor'])
orca.run(['link_daily_processor'])
orca.run(['link_processor'])

orca.run(['write_results'])
orca.run(['print_results'])
