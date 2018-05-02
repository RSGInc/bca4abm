# bca4abm
# See full license in LICENSE.txt.

import logging

import os.path
import numpy as np
import pandas as pd

from activitysim.core import inject

from bca4abm import bca4abm as bca


logger = logging.getLogger(__name__)


@inject.table()
def households(data_dir, settings, hh_chunk_size):

    logger.debug("reading households table")

    base_households = bca.read_csv_table(table_name="base_households",
                                         index_col="household_id",
                                         data_dir=data_dir,
                                         settings=settings)

    build_households = bca.read_csv_table(table_name="build_households",
                                          index_col="household_id",
                                          data_dir=data_dir,
                                          settings=settings)

    households = pd.merge(base_households, build_households, left_index=True, right_index=True)

    # - assign chunk_ids
    chunk_ids = pd.Series(range(len(households)), households.index)
    if hh_chunk_size > 0:
        chunk_ids = np.floor(chunk_ids.div(hh_chunk_size)).astype(int)
    assert 'chunk_id' not in households.columns
    households['chunk_id'] = chunk_ids

    return households


inject.broadcast(cast='households',
                 onto='persons',
                 cast_index=True,
                 onto_on='household_id')
