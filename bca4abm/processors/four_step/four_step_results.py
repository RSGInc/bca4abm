# bca4abm
# See full license in LICENSE.txt.

import logging

import os
import pandas as pd

from activitysim.core import inject


logger = logging.getLogger(__name__)

@inject.step()
def finalize_four_step_results(output_dir, aggregate_results, summary_results, settings):

    pass
