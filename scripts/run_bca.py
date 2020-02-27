# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.
"""run_bca.py

This is a template script for running the bca4abm program on a set of config/data folders. It is
a modifiable version of the `bca4abm run` command that can be customized to fit more specific needs.
One such usage could be to use pipeline tables before they are closed.

useage: python path/to/scripts/run_bca.py
"""
from __future__ import print_function

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


# Add (and handle) 'standard' activitysim arguments:
#     --config : specify path to config_dir. This may be specified several times.
#     --data   : specify path to data_dir. This may be specified several times.
#     --output : specify path to output_dir
#     --resume : resume_after
handle_standard_args()

tracing.config_logger()
tracing.delete_csv_files()  # only modifies output_dir
warnings.simplefilter("always")
logging.captureWarnings(capture=True)

t0 = tracing.print_elapsed_time()  # start timer

models = setting('models')

# If you provide a resume_after argument to pipeline.run
# the pipeline manager will attempt to load checkpointed tables from the checkpoint store
# and resume pipeline processing on the next submodel step after the specified checkpoint
resume_after = setting('resume_after', None)

if resume_after:
    print('resume_after', resume_after)


pipeline.run(models=models, resume_after=resume_after)

print('Pipeline tables:')
for table in pipeline.orca_dataframe_tables():
    print(' ' + table)

# tables will no longer be available after pipeline is closed
pipeline.close_pipeline()

tracing.print_elapsed_time("all models", t0)  # end timer
