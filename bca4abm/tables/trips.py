import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def base_trips(data_dir, settings):

    trips = bca.read_csv_table(data_dir, settings, table_name='basetrips')
    trips_alt = bca.read_csv_table(data_dir, settings, table_name='basetrips_buildlos')

    trips_merged = pd.merge(trips, trips_alt, on=['hh_id', 'person_idx',
                                                  'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'])
    trips_merged['build'] = 0
    trips_merged['base'] = 1
    return trips_merged


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def build_trips(data_dir, settings):
    trips = bca.read_csv_table(data_dir, settings, table_name='buildtrips')
    trips_alt = bca.read_csv_table(data_dir, settings, table_name='buildtrips_baselos')

    trips_merged = pd.merge(trips, trips_alt, on=['hh_id', 'person_idx',
                                                  'tour_idx', 'half_tour_idx', 'half_tour_seg_idx'])
    trips_merged['build'] = 1
    trips_merged['base'] = 0
    return trips_merged


@orca.table(cache=True)
def disaggregate_trips(base_trips, build_trips):

    build = build_trips.to_frame()
    base = base_trips.to_frame()

    # print "disaggregate_trips - appending %s base and %s build" % (base.shape[0], build.shape[0])

    df = base.append(build, ignore_index=True)

    # TODO - aids debugging if we can sort merged versions of this table in base+build csv order
    df['index1'] = df.index

    return df


orca.broadcast(cast='persons_merged',
               onto='disaggregate_trips',
               cast_on=['hh_id', 'person_idx'],
               onto_on=['hh_id', 'person_idx'])


@orca.table()
def trips_with_demographics(disaggregate_trips, persons_merged):
    return orca.merge_tables(target=disaggregate_trips.name,
                             tables=[disaggregate_trips, persons_merged])
