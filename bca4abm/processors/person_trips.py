import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from bca4abm import tracing

from ..util.misc import add_result_columns, add_summary_results

"""
Person trips processor
"""


@orca.injectable()
def person_trips_spec(configs_dir):
    f = os.path.join(configs_dir, 'person_trips.csv')
    return bca.read_assignment_spec(f)


@orca.step()
def person_trips_processor(trips_with_demographics,
                           person_trips_spec,
                           coc_column_names,
                           settings,
                           chunk_size,
                           trace_hh_id):
    """
    Compute disaggregate trips benefits
    """

    trips_df = trips_with_demographics.to_frame()

    tracing.info(__name__,
                 "Running person_trips_processor with %d trips (chunk size = %s)"
                 % (len(trips_with_demographics), chunk_size))

    # eval person_trips_spec in context of trips_with_demographics
    locals_dict = bca.assign_variables_locals(settings, 'person_trips')
    locals_dict['trips'] = trips_df

    trace_rows = trace_hh_id and trips_df['hh_id'] == trace_hh_id

    coc_summary, trace_results, trace_assigned_locals = \
        bca.eval_group_and_sum(assignment_expressions=person_trips_spec,
                               df=trips_df,
                               locals_dict=locals_dict,
                               df_alias='trips',
                               group_by_column_names=coc_column_names,
                               chunk_size=chunk_size,
                               trace_rows=trace_rows)

    result_prefix = 'PT_'
    add_result_columns("coc_results", coc_summary, result_prefix)
    add_summary_results(coc_summary, prefix=result_prefix, spec=person_trips_spec)

    if trace_hh_id:

        if trace_results is not None:

            # FIXME - moved this into assign_variables
            # add trips_df columns to trace_results
            # trace_results = pd.concat([trips_df[trace_rows], trace_results], axis=1)

            tracing.write_csv(trace_results,
                              file_name="person_trips",
                              index_label='trip_id',
                              column_labels=['label', 'trip'])

        if trace_assigned_locals is not None:

            tracing.write_locals(trace_assigned_locals,
                                 file_name="person_trips_locals")
