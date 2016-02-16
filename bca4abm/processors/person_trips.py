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
    f = os.path.join(configs_dir, 'configs', "person_trips.csv")
    return bca.read_assignment_spec(f)


@orca.step()
def person_trips_processor(bca_base_trips_with_demographics, person_trips_spec, settings):

    print "---------- person_trips_processor"

    # create synthetic column in python

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    bca_base_trips_with_demographics = bca_base_trips_with_demographics.to_frame()

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d = {'settings': settings}

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results = bca.assign_variables(assignment_expressions=person_trips_spec,
                                   df=bca_base_trips_with_demographics,
                                   locals_d=locals_d)

    assert "travel_time" in results.columns
    assert "travel_time_alt" in results.columns

    # print "\n### person_trips_processor - results of the expressions for each row in table"
    # print results
    #
    # print "\n### person_trips_processor - person_trips_spec"
    # print person_trips_spec

    add_assigned_columns("bca_base_trips", results)
