import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_grouped_results

"""
auto ownership processor
"""


@orca.injectable()
def auto_ownership_spec(configs_dir):
    f = os.path.join(configs_dir, "auto_ownership.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def auto_ownership_processor(persons_merged, auto_ownership_spec, settings):

    print "---------- auto_ownership_processor"

    persons_df = persons_merged.to_frame()

    locals_d = bca.assign_variables_locals(settings, settings_locals='locals_auto_ownership')
    locals_d['persons'] = persons_df

    assigned_columns = bca.assign_variables(assignment_expressions=auto_ownership_spec,
                                            df=persons_df,
                                            locals_d=locals_d)

    # add assigned columns to local persons_df df
    persons_df = pd.concat([persons_df, assigned_columns], axis=1)

    add_grouped_results(persons_df, assigned_columns.columns,
                        prefix='AO_', spec=auto_ownership_spec)

    if settings.get("dump", False) and settings.get("dump_auto_ownership", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'auto_ownership.csv')
        persons_df = persons_df[['hh_id', 'person_idx'] + list(assigned_columns.columns)]
        persons_df.to_csv(csv_file_name, index=False)
