import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_grouped_results

from ..util.misc import add_result_columns, add_summary_results

from bca4abm import tracing

"""
physical activity processor
"""


@orca.injectable()
def physical_activity_trip_spec(configs_dir):
    f = os.path.join(configs_dir, "physical_activity_trip.csv")
    return bca.read_assignment_spec(f)


@orca.injectable()
def physical_activity_person_spec(configs_dir):
    f = os.path.join(configs_dir, "physical_activity_person.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def physical_activity_processor(trips_with_demographics,
                                persons_merged,
                                physical_activity_trip_spec,
                                physical_activity_person_spec,
                                coc_column_names,
                                settings,
                                hh_chunk_size,
                                trace_hh_id):

    chunk_id_col = 'chunk_id'
    trips_df = trips_with_demographics.to_frame()
    persons_df = persons_merged.to_frame()

    tracing.info(__name__,
                 "Running physical_activity_processor with %d trips for %d persons "
                 "(hh_chunk_size size = %s)"
                 % (len(trips_df), len(persons_df), hh_chunk_size))

    if coc_column_names == [None]:
        raise RuntimeError("physical_activity_processor: coc_column_names not initialized")

    locals_dict = bca.assign_variables_locals(settings, 'locals_physical_activity')
    trip_trace_rows = trace_hh_id and trips_df['hh_id'] == trace_hh_id

    coc_summary = None
    chunks = 0

    # because the persons table doesn't have an identity column
    # we need to use a compound key to group trips by person
    person_identity_columns = ['hh_id', 'person_idx']

    # iterate over trips df chunked by hh_id
    for chunk_id, trips_chunk, trace_rows_chunk \
            in bca.chunked_df(trips_df, trip_trace_rows, chunk_size=None):

        chunks += 1

        # slice persons_df for this chunk
        persons_chunk = persons_df[persons_df[chunk_id_col] == chunk_id]

        trip_activity, trace_results = \
            bca.assign_variables(physical_activity_trip_spec,
                                 trips_chunk,
                                 locals_dict=locals_dict,
                                 df_alias='trips',
                                 trace_rows=trace_rows_chunk)

        if trace_results is not None:
            tracing.write_csv(trace_results,
                              file_name="physical_activity_processor_trips",
                              index_label='trip_id',
                              columns=None,
                              column_labels=['label', 'trip'],
                              transpose=True)

        # concat the person_group_by_column_names columns into trip_activity
        trip_activity = pd.concat([trips_chunk[person_identity_columns], trip_activity], axis=1)

        # sum trip activity for each unique person
        trip_activity = trip_activity.groupby(person_identity_columns).sum()
        trip_activity.reset_index(inplace=True)

        # merge person-level trip activity sums into persons_chunk
        persons_chunk = pd.merge(persons_chunk, trip_activity, on=person_identity_columns)

        person_trace_rows = trace_hh_id and persons_chunk['hh_id'] == trace_hh_id

        person_activity, trace_results = \
            bca.assign_variables(physical_activity_person_spec,
                                 persons_chunk,
                                 locals_dict=locals_dict,
                                 df_alias='persons',
                                 trace_rows=person_trace_rows)

        if trace_results is not None:
            tracing.write_csv(trace_results,
                              file_name="physical_activity_processor_persons",
                              index_label='persons_merged_table_index',
                              column_labels=['label', 'person'],
                              transpose=True)

        # concat in the coc columns
        person_activity = pd.concat([persons_chunk[coc_column_names], person_activity], axis=1)

        # summarize the chunk by coc
        chunk_summary = person_activity.groupby(coc_column_names).sum()

        # accumulate chunk_summaries in df
        if coc_summary is None:
            coc_summary = chunk_summary
        else:
            coc_summary = pd.concat([coc_summary, chunk_summary], axis=0)

    if chunks > 1:
        # squash the accumulated chunk summaries by reapplying group and sum
        coc_summary.reset_index(inplace=True)
        coc_summary = coc_summary.groupby(coc_column_names).sum()

    result_prefix = 'PA_'
    add_result_columns("coc_results", coc_summary, result_prefix)
    add_summary_results(coc_summary, prefix=result_prefix, spec=physical_activity_person_spec)
