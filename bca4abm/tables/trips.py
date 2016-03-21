import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


def read_merged_trips(table_name, alt_table_name, data_dir, input_source, settings):

    # like bca.read_csv_or_stored_table except stores and retrieves merged table and alt together

    if input_source in ['read_from_csv', 'update_store_from_csv']:

        trips = bca.read_csv_or_stored_table(table_name=table_name,
                                             data_dir=data_dir,
                                             input_source=input_source,
                                             settings=settings)

        trips_alt = bca.read_csv_or_stored_table(table_name=alt_table_name,
                                                 data_dir=data_dir,
                                                 input_source=input_source,
                                                 settings=settings)

        trips_merged = pd.merge(trips, trips_alt, on=['hh_id',
                                                      'person_idx',
                                                      'tour_idx',
                                                      'half_tour_idx',
                                                      'half_tour_seg_idx'])

        if input_source == 'update_store_from_csv':
            print "updating store with table %s" % table_name
            with orca.eval_variable('input_store_for_update') as input_store:
                input_store[table_name] = trips_merged
    else:
        with orca.eval_variable('input_store_for_read') as input_store:
            print "reading table %s from store" % table_name
            trips_merged = input_store[table_name]

    return trips_merged


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def base_trips(data_dir, input_source, settings):

    trips_merged = read_merged_trips(table_name="basetrips",
                                     alt_table_name="basetrips_buildlos",
                                     data_dir=data_dir,
                                     input_source=input_source,
                                     settings=settings)

    trips_merged['build'] = 0
    trips_merged['base'] = 1
    return trips_merged


# this caches all the columns that are computed on the trips table
@orca.table(cache=True)
def build_trips(data_dir, input_source, settings):

    trips_merged = read_merged_trips(table_name="buildtrips",
                                     alt_table_name="buildtrips_baselos",
                                     data_dir=data_dir,
                                     input_source=input_source,
                                     settings=settings)

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
