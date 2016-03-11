import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def households(data_dir, settings):

    base_households = bca.read_csv_table(
        data_dir, settings,
        table_name="base_households",
        index_col="hh_id",
        column_map="base_households_column_map")

    # print "\nbase_households\n", base_households

    build_households = bca.read_csv_table(
        data_dir, settings,
        table_name="build_households",
        index_col="hh_id",
        column_map="build_households_column_map")

    # print "\nbuild_households\n", build_households

    households = pd.merge(base_households, build_households, left_index=True, right_index=True)

    return households


orca.broadcast(cast='households',
               onto='persons',
               cast_index=True,
               onto_on='hh_id')
