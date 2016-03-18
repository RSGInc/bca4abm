import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_assigned_columns, add_grouped_results

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

    # eval person_trips_spec in context of trips_with_demographics
    locals_d = bca.assign_variables_locals(settings, settings_locals='locals_person_trips')
    locals_d['trips'] = trips_df

    assigned_columns = bca.assign_variables(assignment_expressions=person_trips_spec,
                                            df=trips_df,
                                            locals_d=locals_d)

    # add assigned columns to local trips df
    trips_df = pd.concat([trips_df, assigned_columns], axis=1)

    add_grouped_results(trips_df, assigned_columns.columns,
                        prefix='PT_', spec=person_trips_spec)

    if settings.get("dump", False):
        trips_df.sort_values(['index1'], inplace=True)
        output_dir = orca.eval_variable('output_dir')

        csv_file_name = os.path.join(output_dir, 'trips_with_demographics.csv')
        print "writing", csv_file_name
        trips_df.to_csv(csv_file_name, index=False)
