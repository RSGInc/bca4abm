import os
import orca

from activitysim import activitysim as asim
# from activitysim.util.misc import add_dependent_columns


"""
Demographics processor
"""


@orca.injectable()
def demographics_spec(configs_dir):
    f = os.path.join(configs_dir, 'configs', "demographics.csv")
    return asim.read_model_spec(f).fillna(0)


@orca.step()
def demographics_processor(bca_households, bca_persons, bca_persons_merged):

    print "---------- demographics_processor"

    households = bca_households.to_frame()
    # print households

    persons = bca_persons.to_frame()
    # print persons

    print bca_persons_merged.to_frame()
