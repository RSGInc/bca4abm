import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx


from bca4abm import bca4abm as bca
from ..util.misc import add_assigned_columns, add_result_columns, add_summary_results
from bca4abm.util.misc import get_setting

from bca4abm import tracing


"""
Aggregate demographics processor
"""


@orca.injectable()
def aggregate_demographics_spec(configs_dir):

    f = os.path.join(configs_dir, "aggregate_demographics.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def aggregate_demographics_processor(zone_cvals, aggregate_demographics_spec,
                                     settings, trace_od):

    print "---------- aggregate_demographics_processor"

    zone_cvals_df = zone_cvals.to_frame()

    tracing.info(__name__,
                 "Running aggregate_demographics_processor with %d zones"
                 % (len(zone_cvals_df), ))

    if trace_od:
        trace_orig, trace_dest = trace_od
        trace_od_rows = (zone_cvals_df.index == trace_orig) | (zone_cvals_df.index == trace_dest)
    else:
        trace_od_rows = None

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = bca.assign_variables_locals(settings, 'aggregate_demographics')

    trace_rows = None

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results, trace_results, trace_assigned_locals = \
        bca.assign_variables(aggregate_demographics_spec,
                             zone_cvals_df,
                             locals_dict,
                             df_alias='cvals',
                             trace_rows=trace_od_rows)

    orca.add_table('zone_demographics', pd.DataFrame(index=results.index))
    add_assigned_columns("zone_demographics", results)

    # create table with coc columns as indexes and a single column 'persons' with counts
    # index                        persons
    # coc_poverty coc_age
    # False       False            20
    #             True              3
    # True        False             4
    coc_columns = ['coc_none', 'coc_age_only', 'coc_poverty_only', 'coc_poverty_age']
    coc_grouped = pd.DataFrame(
        {
            'coc_poverty': [False, False, True, True],
            'coc_age': [False, True, False, True],
            'persons': [results[c].sum() for c in coc_columns]
        }
    )

    orca.add_table('coc_results', pd.DataFrame(index=coc_grouped.index))
    add_result_columns('coc_results', coc_grouped)

    add_summary_results(coc_grouped)

    if trace_results is not None:

        tracing.write_csv(trace_results,
                          file_name="aggregate_demographics",
                          index_label='zone',
                          column_labels=['label', 'zone'])

    if trace_assigned_locals is not None:

        tracing.write_locals(trace_assigned_locals,
                             file_name="aggregate_demographics_locals")
