import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def households(data_dir, input_source, settings):

    base_households = bca.read_csv_or_stored_table(table_name="base_households",
                                                   index_col="hh_id",
                                                   data_dir=data_dir,
                                                   input_source=input_source,
                                                   settings=settings)

    # base_households = bca.read_csv_table(
    #     data_dir, settings,
    #     table_name="base_households",
    #     index_col="hh_id")

    # print "\nbase_households\n", base_households

    build_households = bca.read_csv_or_stored_table(table_name="build_households",
                                                    index_col="hh_id",
                                                    data_dir=data_dir,
                                                    input_source=input_source,
                                                    settings=settings)

    # build_households = bca.read_csv_table(
    #     data_dir, settings,
    #     table_name="build_households",
    #     index_col="hh_id")

    # print "\nbuild_households\n", build_households

    households = pd.merge(base_households, build_households, left_index=True, right_index=True)

    return households


# this assigns a chunk_id to each household based on the chunk_size setting
@orca.column("households", cache=True)
def chunk_id(households, hh_chunk_size):

    chunk_ids = pd.Series(range(len(households)), households.index)

    if hh_chunk_size > 0:
        chunk_ids = np.floor(chunk_ids.div(hh_chunk_size)).astype(int)

    return chunk_ids


orca.broadcast(cast='households',
               onto='persons',
               cast_index=True,
               onto_on='hh_id')
