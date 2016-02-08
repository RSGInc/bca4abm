import numpy as np
import pandas as pd
import orca
import os
import yaml

# from activitysim import defaults
# this has the unfortunate side effect of loading all the injectable tables and functions
# from defaults.tables and defaults.models because defaults.__init__ imports tables and models

import activitysim.defaults.misc


@orca.injectable(cache=True)
def output_store(output_dir, settings):
    if "output_store" in settings:
        return pd.HDFStore(os.path.join(output_dir, "output", settings["output_store"]))
    else:
        return None


def run_bca():

    # expects injectable "configs_dir"
    # expects injectable "data_dir"

    settings = orca.eval_variable('settings')

    if 'store' not in settings:
        orca.add_injectable("store", None)

    orca.run(["demographics_processor"])

    if "output_store" in settings:
        print "output_store"

        output_store = orca.eval_variable('output_store')

        print "   writing bca_households..."
        households = orca.eval_variable('bca_households').to_frame()
        output_store['bca_households'] = households

        print "   writing bca_persons..."
        persons = orca.eval_variable('bca_persons').to_frame()
        output_store['bca_persons'] = persons
