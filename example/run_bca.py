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
orca.run(["write_output_store"])
