import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx


from bca4abm import bca4abm as bca
from ...util.misc import add_assigned_columns, add_aggregate_results

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
    """

    Parameters
    ----------
    zone_cvals : orca table
        input zone demographics

    """

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

    coc_silos = settings.get('coc_silos', None)
    if coc_silos is None:
        raise RuntimeError("coc_silos not defined in settings")
    orca.add_injectable("coc_silos", coc_silos)

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

    # orca.add_table('zone_demographics', pd.DataFrame(index=results.index))
    # add_assigned_columns("zone_demographics", results)
    orca.add_table('zone_demographics', results)

    if settings.get("dump", False):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'zone_demographics.csv')
        print "writing", csv_file_name
        results.to_csv(csv_file_name, index=False)

    # expression file can use silos column to designate result targets (e.g. count of households)
    add_aggregate_results(results, aggregate_demographics_spec, source='aggregate_demographics')

    if trace_results is not None:

        # tracing.write_csv(results,
        #                   file_name="results",
        #                   transpose=False)

        tracing.write_csv(trace_results,
                          file_name="aggregate_demographics",
                          index_label='zone',
                          column_labels=['label', 'zone'])

    if trace_assigned_locals is not None:

        tracing.write_locals(trace_assigned_locals,
                             file_name="aggregate_demographics_locals")
