import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca


@orca.step()
def initialize_output_store(output_dir, output_store_file_name):

    output = pd.HDFStore(os.path.join(output_dir, output_store_file_name),
                         mode='w')
    output.close()
