import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx


from bca4abm import bca4abm as bca
from ..util.misc import add_assigned_columns, missing_columns, add_result_columns, add_summary_results
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
def aggregate_demographics_processor(zone_demographics, aggregate_demographics_spec, settings):

    print "---------- aggregate_demographics_processor"

    zones_df = zone_demographics.to_frame()

    tracing.info(__name__,
                 "Running aggregate_demographics_processor with %d zones"
                 % (len(zones_df), ))

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = bca.assign_variables_locals(settings, 'locals_demographics')


    trace_rows =None

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results, trace_results = bca.assign_variables(aggregate_demographics_spec,
                                                  zones_df,
                                                  locals_dict,
                                                  df_alias='zones',
                                                  trace_rows=trace_rows)

    print "results:", results


    # add assigned columns to persons as they are needed by downstream processors
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

    # if trace_hh_id:
    #
    #     if trace_results is not None:
    #
    #         tracing.write_csv(trace_results,
    #                           file_name="demographics_processor",
    #                           index_label='person_idx',
    #                           column_labels=['label', 'person'])
