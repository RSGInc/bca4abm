import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_bca_households(data_dir, store, settings):

    return bca.get_raw_table("bca_households",
                             index_col="hh_id",
                             column_map="bca_households_column_map")


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def bca_households(raw_bca_households):
    return raw_bca_households.to_frame()


orca.broadcast(cast='bca_households',
               onto='bca_persons',
               cast_index=True,
               onto_on='hh_id')
