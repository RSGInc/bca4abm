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


df = orca.get_table('build_trips').to_frame()

print "build_trips", df.shape[0]

print df
