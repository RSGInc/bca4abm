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
    assigned_columns = bca.assign_variables(assignment_expressions=person_trips_spec,
                                            df=df,
                                            locals_d=locals_d)

    assert "travel_time_benefit" in assigned_columns.columns
    assert "monetized_travel_time_benefit" in assigned_columns.columns

    add_assigned_columns("trips_with_demographics", assigned_columns)

    trips_df = orca.get_table('trips_with_demographics').to_frame()
    grouped = trips_df.groupby(['coc_age', 'coc_poverty'])
    aggregations = {
        'monetized_travel_time_benefit': 'sum',
        'travel_time_benefit': 'sum',
    }
    grouped = grouped.agg(aggregations)
    grouped.reset_index(inplace=True)

    with orca.eval_variable('output_store') as output_store:
        grouped.insert(loc=0, column='scenario', value=settings['scenario_label'])
        output_store['person_trips'] = grouped

    if settings.get("dump", False):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'trips_with_demographics.csv')
        print "writing", csv_file_name
        trips_df.sort(['index1']).to_csv(csv_file_name)
