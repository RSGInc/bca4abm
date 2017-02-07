import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx


from bca4abm import bca4abm as bca

from ...util.misc import add_aggregate_results

from bca4abm import tracing


"""
Aggregate zone processor
"""


@orca.injectable()
def aggregate_zone_spec(configs_dir):

    f = os.path.join(configs_dir, "aggregate_zone.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def aggregate_zone_processor(zones, aggregate_zone_spec, settings, trace_od):
    """
    zones: orca table

    zone data for base and build scenario dat files combined into a single dataframe
    with columns names prefixed with 'base_' or 'build_' indexed by ZONE
    """

    print "---------- aggregate_zone_processor"

    zones_df = zones.to_frame()

    tracing.info(__name__,
                 "Running aggregate_zone_processor with %d zones"
                 % (len(zones_df.index), ))

    if trace_od:
        trace_orig, trace_dest = trace_od
        trace_od_rows = (zones_df.index == trace_orig) | (zones_df.index == trace_dest)
    else:
        trace_od_rows = None

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = bca.assign_variables_locals(settings, 'aggregate_zone')

    trace_rows = None

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results, trace_results, trace_assigned_locals = \
        bca.assign_variables(aggregate_zone_spec,
                             zones_df,
                             locals_dict,
                             df_alias='zones',
                             trace_rows=trace_od_rows)

    add_aggregate_results(results, aggregate_zone_spec, source='aggregate_zone')

    if settings.get("dump", False):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'aggregate_zone_benefits.csv')
        print "writing", csv_file_name
        results.to_csv(csv_file_name, index=False)

    if trace_results is not None:

        # tracing.write_csv(results,
        #                   file_name="aggregate_zone_results",
        #                   transpose=False)

        tracing.write_csv(trace_results,
                          file_name="aggregate_zone",
                          index_label='zone',
                          column_labels=['label', 'zone'])

    if trace_assigned_locals is not None:

        tracing.write_locals(trace_assigned_locals,
                             file_name="aggregate_zone_locals")
