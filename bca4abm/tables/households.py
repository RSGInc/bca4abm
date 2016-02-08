import os.path
import numpy as np
import orca
import pandas as pd

from ..util.misc import read_bca_table


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def bca_households_internal(data_dir, store, settings):

    if "bca_households" in settings:
        df = read_bca_table("bca_households", 'hh_id', data_dir, settings)
    else:
        df = store["bca_households"]

    return df


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def bca_households(bca_households_internal):
    return bca_households_internal.to_frame()


orca.broadcast(cast='bca_households',
               onto='bca_persons',
               cast_index=True,
               onto_on='hh_id')
