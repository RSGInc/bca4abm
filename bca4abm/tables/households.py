import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_households(data_dir, settings):

    return bca.read_csv_table(data_dir, settings,
                              table_name="households",
                              index_col="hh_id",
                              column_map="households_column_map")


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def households(raw_households):
    return raw_households.to_frame()


orca.broadcast(cast='households',
               onto='persons',
               cast_index=True,
               onto_on='hh_id')
