import os
import orca


from bca4abm import bca4abm as bca
from ..util.misc import add_assigned_columns
from ..util.misc import add_dependent_columns

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

    # create synthetic column in python

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    persons_merged = persons_merged.to_frame()

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d = None

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results = bca.assign_variables(demographics_spec, persons_merged, locals_d)

    # print "\n### demographics_processor - results of the expressions for each row in table"
    # print results
    #
    # print "\n### demographics_processor - demographics_spec"
    # print demographics_spec

    add_assigned_columns("persons", results)

    # FIXME remove this if there are no demographics columns dependent
    add_dependent_columns("persons", "persons_demographics")

    if settings.get("dump", False):
        persons_merged = orca.get_table('persons_merged').to_frame()
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'persons_merged.csv')
        print "writing", csv_file_name
        persons_merged.to_csv(csv_file_name)
