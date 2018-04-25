# bca4abm
# See full license in LICENSE.txt.

import logging

import os.path
import numpy as np
import pandas as pd

from activitysim.core import inject


logger = logging.getLogger(__name__)


@inject.table()
def summary_results():

    logger.debug("initializing empty summary_results table")

    return pd.DataFrame(index=[0])


@inject.table()
def coc_results():

    logger.debug("initializing empty coc_results table")

    return pd.DataFrame()


@inject.injectable()
def coc_column_names():

    raise RuntimeError("coc_column_names not initialized"
                       " - did you forget to run demographics_processor?")


@inject.injectable(cache=True)
def data_dictionary():
    return {}


@inject.table()
def aggregate_results():

    logger.debug("initializing empty aggregate_results table")

    return pd.DataFrame()


@inject.injectable(cache=True)
def coc_silos():

    raise RuntimeError("coc_silos not initialized"
                       " - did you forget to run aggregate_demographics_processor?")
