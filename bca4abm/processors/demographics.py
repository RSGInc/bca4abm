import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from ..util.misc import add_assigned_columns, add_result_columns, add_summary_results

"""
Demographics processor
"""


@orca.injectable()
def demographics_spec(configs_dir):
    f = os.path.join(configs_dir, "demographics.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def demographics_processor(persons_merged, demographics_spec, settings):

    print "---------- demographics_processor"

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    persons_df = persons_merged.to_frame()

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d = bca.assign_variables_locals(settings, settings_locals='locals_demographics')
    locals_d['persons'] = persons_df

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results = bca.assign_variables(demographics_spec, persons_df, locals_d)

    # add assigned columns to persons as they are needed by downstream processors
    add_assigned_columns("persons", results)

    # coc groups with counts
    # TODO - should we allow specifying which assigned columns are coc (e.g. in settings?)
    # for now, assume all assigned columns are coc, but this could cramp modelers style
    # if they want to create additional demographic columns for downstream use that aren't coc
    coc_columns = list(results.columns)
    coc_grouped = results.groupby(coc_columns)
    coc_grouped = coc_grouped[coc_columns[0]].agg({'persons': 'count'})
    orca.add_table('coc_results', pd.DataFrame(index=coc_grouped.index))
    add_result_columns('coc_results', coc_grouped)

    add_summary_results(coc_grouped)

    if settings.get("dump", False) and settings.get("dump_demographics", True):
        persons_merged = orca.get_table('persons_merged').to_frame()
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'persons_merged.csv')
        print "writing", csv_file_name
        persons_merged.to_csv(csv_file_name, index=False)
