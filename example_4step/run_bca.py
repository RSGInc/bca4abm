# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import pandas as pd
import numpy as np
import os
import warnings
import logging


# the following import has the side-effect of registering injectables
from bca4abm import bca4abm as bca

from activitysim.core import tracing
from activitysim.core import pipeline
from activitysim.core import inject

from activitysim.core.config import handle_standard_args
from activitysim.core.config import setting

parent_dir = os.path.dirname(__file__)


# Add (and handle) 'standard' activitysim arguments:
#     --config : specify path to config_dir
#     --output : specify path to output_dir
#     --data   : specify path to data_dir
#     --models : specify run_list name
#     --resume : resume_after
handle_standard_args()

tracing.config_logger()
tracing.delete_csv_files(output_dir=inject.get_injectable('output_dir'))

warnings.simplefilter("always")

logging.captureWarnings(capture=True)

old_settings = np.seterr(divide='raise', over='raise', invalid='raise', under='ignore')
print "numpy.geterr: %s" % np.geterr()


t0 = tracing.print_elapsed_time()

MODELS = setting('models')

# If you provide a resume_after argument to pipeline.run
# the pipeline manager will attempt to load checkpointed tables from the checkpoint store
# and resume pipeline processing on the next submodel step after the specified checkpoint
resume_after = setting('resume_after', None)

if resume_after:
    print "resume_after", resume_after


pipeline.run(models=MODELS, resume_after=resume_after)

# tables will no longer be available after pipeline is closed
pipeline.close_pipeline()

t0 = tracing.print_elapsed_time("all models", t0)

