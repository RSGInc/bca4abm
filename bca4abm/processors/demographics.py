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
    f = os.path.join(configs_dir, 'configs', "demographics.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def demographics_processor(bca_persons_merged, demographics_spec, settings):

    print "---------- demographics_processor"

    # create synthetic column in python

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    persons_merged = bca_persons_merged.to_frame()

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

    add_assigned_columns("bca_persons", results)

    # FIXME remove this if there are no demographics columns dependent
    add_dependent_columns("bca_persons", "persons_demographics")
