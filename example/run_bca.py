import orca
import pandas as pd
import numpy as np
import os

# there is something a little bit too opaque about this:
# the following import has the side-effect of registering injectables
#from activitysim import defaults
#from activitysim.defaults import misc

from bca4abm import bca4abm as bca

#orca.add_injectable("configs_dir", os.path.dirname(__file__))
#orca.add_injectable("data_dir", os.path.dirname(__file__))

orca.add_injectable("configs_dir", '')
orca.add_injectable("data_dir", '')
orca.add_injectable("output_dir", '')

bca.run_bca()
