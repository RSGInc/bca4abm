import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_grouped_results

"""
physical activity processor
"""


@orca.injectable()
def physical_activity_trip_spec(configs_dir):
    f = os.path.join(configs_dir, "physical_activity_trip.csv")
    return bca.read_assignment_spec(f)


@orca.injectable()
def physical_activity_person_spec(configs_dir):
    f = os.path.join(configs_dir, "physical_activity_person.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def physical_activity_processor(trips_with_demographics,
                                persons_merged,
                                physical_activity_trip_spec,
                                physical_activity_person_spec,
                                settings):

    print "---------- physical_activity_processor"

    trips_df = trips_with_demographics.to_frame()

    locals_d = bca.assign_variables_locals(settings, settings_locals='locals_physical_activity')
    locals_d['trips'] = trips_df

    assigned_columns = bca.assign_variables(assignment_expressions=physical_activity_trip_spec,
                                            df=trips_df,
                                            locals_d=locals_d)

    # add assigned columns to local trips df
    trips_df = pd.concat([trips_df, assigned_columns], axis=1)

    # aggregate trips to persons: sum assigned_columns, group by hh_id, person_idx
    summary_columns = assigned_columns.columns
    grouped = trips_df.groupby(['hh_id', 'person_idx'])
    aggregations = {column: 'sum' for column in summary_columns}
    persons_activity_df = grouped.agg(aggregations)
    persons_activity_df.reset_index(inplace=True)

    # merge aggregated assigned_columns to persons_df
    persons_df = persons_merged.to_frame()
    persons_df = pd.merge(persons_df, persons_activity_df, on=['hh_id', 'person_idx'])

    # eval physical_activity_person_spec in context of merged trips_with_demographics
    locals_d = {'settings': settings, 'persons': persons_df}
    if 'locals_physical_activity' in settings:
        locals_d.update(settings['locals_physical_activity'])
    assigned_columns = bca.assign_variables(assignment_expressions=physical_activity_person_spec,
                                            df=persons_df,
                                            locals_d=locals_d)

    # merge aggregated assigned_columns to local persons_df
    persons_df = pd.concat([persons_df, assigned_columns], axis=1)

    add_grouped_results(persons_df, assigned_columns.columns, prefix='PA_')

    if settings.get("dump", False) and settings.get("dump_physical_activity", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'physical_activity.csv')
        persons_df = persons_df[['hh_id', 'person_idx'] + list(assigned_columns.columns)]
        persons_df.to_csv(csv_file_name, index=False)
