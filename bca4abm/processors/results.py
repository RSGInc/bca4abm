import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca


@orca.step()
def initialize_stores(output_dir, output_store_file_name, data_dir, input_store_file_name):

    print "---------- initialize_stores"

    output = pd.HDFStore(os.path.join(output_dir, output_store_file_name), mode='w')
    output.close()

    if orca.eval_variable('input_source') in ['update_store_from_csv']:
        input = pd.HDFStore(os.path.join(data_dir, input_store_file_name), mode='w')
        input.close()


@orca.step()
def print_results():

    print "---------- print_results"

    with orca.eval_variable('output_store_for_read') as hdf:

        for key in hdf.keys():

            print "\n========== %s\n" % key
            print hdf[key]
