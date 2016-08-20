import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_assigned_columns, add_result_columns, add_summary_results

from bca4abm import tracing

"""
Demographics processor
"""


@orca.injectable()
def demographics_spec(configs_dir):
    f = os.path.join(configs_dir, "demographics.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def demographics_processor(persons_merged, demographics_spec, settings,
                           chunk_size,
                           trace_hh_id):

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    persons_df = persons_merged.to_frame()

    tracing.info(__name__,
                 "Running demographics_processor with %d persons (chunk size = %s)"
                 % (len(persons_df), chunk_size))

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = bca.assign_variables_locals(settings, 'locals_demographics')

    trace_rows = trace_hh_id and persons_df['hh_id'] == trace_hh_id

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results, trace_results = bca.assign_variables(demographics_spec,
                                                  persons_df,
                                                  locals_dict,
                                                  df_alias='persons',
                                                  trace_rows=trace_rows)

    # add assigned columns to persons as they are needed by downstream processors
    add_assigned_columns("persons", results)

    # coc groups with counts
    # TODO - should we allow specifying which assigned columns are coc (e.g. in settings?)
    # for now, assume all assigned columns are coc, but this could cramp modelers style
    # if they want to create additional demographic columns for downstream use that aren't coc
    coc_columns = list(results.columns)
    coc_grouped = results.groupby(coc_columns)
    coc_grouped = coc_grouped[coc_columns[0]].agg({'persons': 'count'})
    orca.add_table('coc_results', pd.DataFrame(index=coc_grouped.index))
    add_result_columns('coc_results', coc_grouped)

    add_summary_results(coc_grouped)

    if trace_hh_id:

        if trace_results is not None:

            # add trips_df columns to trace_results
            df = pd.concat([persons_df[trace_rows], trace_results], axis=1)
            tracing.write_csv(df,
                              file_name="demographics_processor",
                              index_label='person_idx',
                              columns=None,
                              column_labels=['label', 'person'],
                              transpose=True)
