import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_bca_trips(data_dir, store, settings):

    # return bca.get_raw_table("bca_base_trips", column_map="bca_trips_column_map")

    base_df = bca.get_raw_table('bca_base_trips', column_map='bca_trips_column_map')
    build_df = bca.get_raw_table('bca_build_trips', column_map='bca_trips_column_map')

    base_df['build'] = 1
    build_df['build'] = -1
    df = pd.concat([base_df, build_df])

    return df


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def bca_trips(raw_bca_trips):
    return raw_bca_trips.to_frame()


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_bca_trips_alt(data_dir, store, settings):

    # return bca.get_raw_table("bca_base_trips_alt", column_map="bca_trips_alt_column_map")

    base_df = bca.get_raw_table('bca_base_trips_alt', column_map='bca_trips_alt_column_map')
    build_df = bca.get_raw_table('bca_build_trips_alt', column_map='bca_trips_alt_column_map')

    base_df['build'] = 1
    build_df['build'] = -1
    df = pd.concat([base_df, build_df])

    return df


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def bca_trips_alt(raw_bca_trips_alt):
    return raw_bca_trips_alt.to_frame()

orca.broadcast(cast='bca_trips_alt',
               onto='bca_trips',
               cast_on=['build', 'hh_id', 'person_idx',
                        'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'],
               onto_on=['build', 'hh_id', 'person_idx',
                        'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'])

orca.broadcast(cast='bca_persons_merged',
               onto='bca_trips_merged',
               cast_on=['hh_id', 'person_idx'],
               onto_on=['hh_id', 'person_idx'])


@orca.table()
def bca_trips_merged(bca_trips, bca_trips_alt):
    return orca.merge_tables(target=bca_trips.name,
                             tables=[bca_trips, bca_trips_alt])


@orca.table()
def bca_trips_with_demographics(bca_trips_merged, bca_persons_merged):
    return orca.merge_tables(target=bca_trips_merged.name,
                             tables=[bca_trips_merged, bca_persons_merged])
