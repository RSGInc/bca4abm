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

    trips_df = trips_with_demographics.to_frame()

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d = {'settings': settings,
                'trips': trips_df}

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the df dataframe
    assigned_columns = bca.assign_variables(assignment_expressions=person_trips_spec,
                                            df=trips_df,
                                            locals_d=locals_d)

    add_assigned_columns("trips_with_demographics", assigned_columns)

    # summarize everything
    summary_columns = assigned_columns.columns

    # TODO - if nobody downstream needs this, just merge locally summarize and discard?
    # reload this so we get the assigned columns
    trips_df = orca.get_table('trips_with_demographics').to_frame()
    grouped = trips_df.groupby(['coc_age', 'coc_poverty'])

    aggregations = {column: 'sum' for column in summary_columns}

    grouped = grouped.agg(aggregations)
    grouped.reset_index(inplace=True)
    grouped.insert(loc=0, column='scenario', value=settings['scenario_label'])

    summary = grouped[summary_columns].sum()
    summary = pd.DataFrame(data=summary).T
    summary.insert(loc=0, column='scenario', value=settings['scenario_label'])
    print "\nsummary\n", summary

    with orca.eval_variable('output_store') as output_store:
        output_store['person_trips_coc'] = grouped
        output_store['person_trips'] = summary

    if settings.get("dump", False):
        trips_df.sort_values(['index1'], inplace=True)

        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'trips_with_demographics.csv')
        print "writing", csv_file_name
        trips_df.to_csv(csv_file_name)

        # print "\n%s\n" % trips_df[ assigned_columns.columns ][ : 10 ]
        #
        # for column in summary_columns:
        #     print column, trips_df[column].sum()
