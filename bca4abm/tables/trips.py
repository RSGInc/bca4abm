import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_bca_base_trips(data_dir, store, settings):
    return bca.get_raw_table("bca_base_trips", column_map="bca_trips_column_map")


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def bca_base_trips(raw_bca_base_trips):
    return raw_bca_base_trips.to_frame()


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_bca_base_trips_alt(data_dir, store, settings):

    return bca.get_raw_table("bca_base_trips_alt", column_map="bca_trips_alt_column_map")


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def bca_base_trips_alt(raw_bca_base_trips_alt):
    return raw_bca_base_trips_alt.to_frame()

orca.broadcast(cast='bca_base_trips_alt',
               onto='bca_base_trips',
               cast_on=['hh_id', 'person_idx', 'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'],
               onto_on=['hh_id', 'person_idx', 'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'])

orca.broadcast(cast='bca_persons_merged',
               onto='bca_base_trips_merged',
               cast_on=['hh_id', 'person_idx'],
               onto_on=['hh_id', 'person_idx'])


@orca.table()
def bca_base_trips_merged(bca_base_trips, bca_base_trips_alt):
    return orca.merge_tables(target=bca_base_trips.name,
                             tables=[bca_base_trips, bca_base_trips_alt])


@orca.table()
def bca_base_trips_with_demographics(bca_base_trips_merged, bca_persons_merged):
    return orca.merge_tables(target=bca_base_trips_merged.name,
                             tables=[bca_base_trips_merged, bca_persons_merged])
