import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_assigned_columns

"""
Person trips processor
"""


@orca.injectable()
def person_trips_spec(configs_dir):
    f = os.path.join(configs_dir, 'person_trips.csv')
    return bca.read_assignment_spec(f)


@orca.step()
def person_trips_processor(trips_with_demographics, person_trips_spec, settings):

    print "---------- person_trips_processor"

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d = {'settings': settings}

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the df dataframe
    df = trips_with_demographics.to_frame()
    results = bca.assign_variables(assignment_expressions=person_trips_spec,
                                   df=df,
                                   locals_d=locals_d)

    assert "travel_time_benefit" in results.columns
    assert "monetized_travel_time_benefit" in results.columns

    add_assigned_columns("trips_with_demographics", results)

    trips_df = orca.get_table('trips_with_demographics').to_frame()
    grouped = trips_df.groupby(['coc_age', 'coc_poverty'])
    aggregations = {
        'monetized_travel_time_benefit': 'sum',
        'travel_time_benefit': 'sum',
    }
    grouped = grouped.agg(aggregations)
    grouped.reset_index(inplace=True)
    grouped['scenario'] = settings['scenario_label']

    grouped.set_index(['scenario', 'coc_age', 'coc_poverty'], inplace=True)

    # print grouped

    # FIXME - PerformanceWarning for coc booleans types
    # your performance may suffer as PyTables will pickle object types that it cannot
    # map directly to c-types [inferred_type->boolean,key->axis1_level2] [items->None]

    with orca.eval_variable('output_store') as output_store:
        output_store['person_trips'] = grouped

    # output_dir = orca.eval_variable('output_dir')
    # trips_df.sort(['index1']).to_csv(os.path.join(output_dir, 'trips_with_demographics.csv') )
