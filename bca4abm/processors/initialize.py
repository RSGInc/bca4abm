# bca4abm
# See full license in LICENSE.txt.

import logging
import os

import pandas as pd
import numpy as np

from activitysim.core import inject

from activitysim.core.steps.output import write_data_dictionary
from activitysim.core.steps.output import write_tables
# from activitysim.core.steps.output import track_skim_usage


logger = logging.getLogger(__name__)


@inject.injectable(cache=True)
def preload_injectables():

    logger.info("preload_injectables")

    # inject.add_step('track_skim_usage', track_skim_usage)
    inject.add_step('write_data_dictionary', write_data_dictionary)
    inject.add_step('write_tables', write_tables)
