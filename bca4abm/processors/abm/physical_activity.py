# bca4abm
# See full license in LICENSE.txt.

import logging

import os
import pandas as pd

from bca4abm import bca4abm as bca
from ...util.misc import add_result_columns, add_summary_results

from activitysim.core import config
from activitysim.core import inject
from activitysim.core import tracing
from activitysim.core import assign


logger = logging.getLogger(__name__)


"""
physical activity processor
"""


@inject.injectable()
def physical_activity_trip_spec(configs_dir):
    f = os.path.join(configs_dir, "physical_activity_trip.csv")
    return bca.read_assignment_spec(f)


@inject.injectable()
def physical_activity_person_spec(configs_dir):
    f = os.path.join(configs_dir, "physical_activity_person.csv")
    return bca.read_assignment_spec(f)


@inject.injectable()
def physical_activity_settings(configs_dir):
    return config.read_model_settings(configs_dir, 'physical_activity.yaml')


@inject.step()
def physical_activity_processor(
        trips_with_demographics,
        persons_merged,
        physical_activity_trip_spec,
        physical_activity_person_spec,
        physical_activity_settings,
        coc_column_names,
        settings,
        hh_chunk_size,
        trace_hh_id):

    """
    Compute physical benefits

    Physical activity benefits generally accrue if the net physical activity for an individual
    exceeds a certain threshold. We calculate individual physical activity based on trips,
    so we need to compute trip activity and then sum up to the person level to calculate benefits.
    We chunk trips by household id to ensure that all of a persons trips are in the same chunk.
    """

    trips_df = trips_with_demographics.to_frame()
    persons_df = persons_merged.to_frame()

    logger.info("Running physical_activity_processor with %d trips for %d persons "
                "(hh_chunk_size size = %s)"
                % (len(trips_df), len(persons_df), hh_chunk_size))

    locals_dict = config.get_model_constants(physical_activity_settings)
    locals_dict.update(config.setting('globals'))

    trip_trace_rows = trace_hh_id and trips_df.household_id == trace_hh_id

    coc_summary = None
    chunks = 0

    # iterate over trips df chunked by hh_id
    for chunk_id, trips_chunk, trace_rows_chunk \
            in bca.chunked_df(trips_df, trip_trace_rows, chunk_size=None):

        chunks += 1

        # slice persons_df for this chunk (chunk_id column merged in from households table)
        persons_chunk = persons_df[persons_df['chunk_id'] == chunk_id]

        trip_activity, trip_trace_results, trip_trace_assigned_locals = \
            assign.assign_variables(physical_activity_trip_spec,
                                    trips_chunk,
                                    locals_dict=locals_dict,
                                    df_alias='trips',
                                    trace_rows=trace_rows_chunk)

        # since tracing is at household level, trace_results will occur in only one chunk
        # we can just write them out when we see them without need to accumulate across chunks
        if trip_trace_results is not None:
            tracing.write_csv(trip_trace_results,
                              file_name="physical_activity_trips",
                              index_label='trip_id',
                              column_labels=['label', 'trip'])

            if trip_trace_assigned_locals:
                tracing.write_csv(trip_trace_assigned_locals,
                                  file_name="physical_activity_trips_locals")

        # sum trip activity for each unique person
        trip_activity = trip_activity.groupby(trips_chunk.person_id).sum()

        # merge person-level trip activity sums into persons_chunk
        persons_chunk = pd.merge(persons_chunk, trip_activity,
                                 left_index=True, right_index=True)

        # trace rows array for this chunk
        person_trace_rows = trace_hh_id and persons_chunk['household_id'] == trace_hh_id

        person_activity, person_trace_results, person_trace_assigned_locals = \
            assign.assign_variables(physical_activity_person_spec,
                                    persons_chunk,
                                    locals_dict=locals_dict,
                                    df_alias='persons',
                                    trace_rows=person_trace_rows)

        # since tracing is at household level, trace_results will occur in only one chunk
        # we can just write them out when we see them without need to accumulate across chunks
        if person_trace_results is not None:
            tracing.write_csv(person_trace_results,
                              file_name="physical_activity_persons",
                              index_label='persons_merged_table_index',
                              column_labels=['label', 'person'])

            if person_trace_assigned_locals:
                tracing.write_csv(person_trace_assigned_locals,
                                  file_name="physical_activity_persons_locals")

        # concat in the coc columns and summarize the chunk by coc
        person_activity = pd.concat([persons_chunk[coc_column_names], person_activity], axis=1)
        chunk_summary = person_activity.groupby(coc_column_names).sum()

        # accumulate chunk_summaries in df
        if coc_summary is None:
            coc_summary = chunk_summary
        else:
            coc_summary = pd.concat([coc_summary, chunk_summary], axis=0)

        # end of chunk loop

    if chunks > 1:
        # squash the accumulated chunk summaries by reapplying group and sum
        coc_summary.reset_index(inplace=True)
        coc_summary = coc_summary.groupby(coc_column_names).sum()

    result_prefix = 'PA_'
    add_result_columns("coc_results", coc_summary, result_prefix)
    add_summary_results(coc_summary, prefix=result_prefix, spec=physical_activity_person_spec)
