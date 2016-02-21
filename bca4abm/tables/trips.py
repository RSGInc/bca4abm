import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_trips(data_dir, settings):

    base_df = bca.read_csv_table(data_dir, settings, table_name='bca_base_trips',
                                 column_map='trips_column_map')
    build_df = bca.read_csv_table(data_dir, settings, table_name='bca_build_trips',
                                  column_map='trips_column_map')

    base_df['build'] = 1
    build_df['build'] = -1
    df = pd.concat([base_df, build_df])

    return df


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def trips(raw_trips):
    return raw_trips.to_frame()


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_trips_alt(data_dir, settings):

    base_df = bca.read_csv_table(data_dir, settings, table_name='bca_base_trips_alt',
                                 column_map='trips_alt_column_map')
    build_df = bca.read_csv_table(data_dir, settings, table_name='bca_build_trips_alt',
                                  column_map='trips_alt_column_map')

    base_df['build'] = 1
    build_df['build'] = -1
    df = pd.concat([base_df, build_df])

    return df


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def trips_alt(raw_trips_alt):
    return raw_trips_alt.to_frame()


orca.broadcast(cast='trips_alt',
               onto='trips',
               cast_on=['build', 'hh_id', 'person_idx',
                        'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'],
               onto_on=['build', 'hh_id', 'person_idx',
                        'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'])

orca.broadcast(cast='persons_merged',
               onto='trips_merged',
               cast_on=['hh_id', 'person_idx'],
               onto_on=['hh_id', 'person_idx'])


@orca.table()
def trips_merged(trips, trips_alt):
    return orca.merge_tables(target=trips.name,
                             tables=[trips, trips_alt])


@orca.table()
def trips_with_demographics(trips_merged, persons_merged):
    return orca.merge_tables(target=trips_merged.name,
                             tables=[trips_merged, persons_merged])


@orca.column("trips")
def tour_type(trips, settings):
    return trips.tour_purpose.map(settings["tour_purpose_map"])
