import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_result_columns, add_summary_results

from bca4abm import tracing

"""
auto ownership processor
"""


@orca.injectable()
def auto_ownership_spec(configs_dir):
    f = os.path.join(configs_dir, "auto_ownership.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def auto_ownership_processor(persons_merged,
                             auto_ownership_spec,
                             settings,
                             coc_column_names,
                             chunk_size,
                             trace_hh_id):

    """
    Compute auto ownership benefits
    """

    persons_df = persons_merged.to_frame()

    tracing.info(__name__,
                 "Running auto_ownership_processor with %d persons (chunk size = %s)"
                 % (len(persons_df), chunk_size))

    locals_dict = bca.assign_variables_locals(settings, 'locals_auto_ownership')

    trace_rows = trace_hh_id and persons_df['hh_id'] == trace_hh_id

    coc_summary, trace_results = bca.eval_group_and_sum(assignment_expressions=auto_ownership_spec,
                                                        df=persons_df,
                                                        locals_dict=locals_dict,
                                                        df_alias='persons',
                                                        group_by_column_names=coc_column_names,
                                                        chunk_size=chunk_size,
                                                        trace_rows=trace_rows)

    result_prefix = 'AO_'
    add_result_columns("coc_results", coc_summary, result_prefix)
    add_summary_results(coc_summary, prefix=result_prefix, spec=auto_ownership_spec)

    if trace_hh_id:

        if trace_results is not None:

            tracing.write_csv(trace_results,
                              file_name="auto_ownership_processor",
                              index_label='person_id',
                              column_labels=['label', 'person'])
