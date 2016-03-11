import os
import orca
import pandas as pd

from bca4abm import bca4abm as bca


@orca.step()
def initialize_output_store(output_dir, output_store_file_name):

    print "---------- initialize_output_store"

    output = pd.HDFStore(os.path.join(output_dir, output_store_file_name),
                         mode='w')
    output.close()


@orca.step()
def write_results(output_dir, coc_results, summary_results, settings):

    print "---------- write_results"

    with orca.eval_variable('output_store') as output_store:
        # reset multilevel index on coc_results (KISS)
        output_store['coc_results'] = coc_results.to_frame().reset_index(drop=False)
        output_store['summary_results'] = summary_results.to_frame()

    if settings.get("dump", False):

        # coc_silos
        if coc_results.index.names != [None]:

            coc_silos = pd.DataFrame()
            coc_column_names = coc_results.index.names
            assigned_column_names = coc_results.columns
            df = coc_results.to_frame().reset_index(drop=False)

            # one column for each coc column
            for coc in coc_column_names:
                coc_silos[coc] = df[df[coc]][assigned_column_names].sum()
            # plus an extra column for 'any coc'
            coc_silos['any_coc'] = df[df[coc_column_names].any(axis=1)][assigned_column_names].sum()
            coc_silos.sort_index(inplace=True)

            csv_file_name = os.path.join(output_dir, 'coc_silos.csv')
            coc_silos.to_csv(csv_file_name, index=True)

        csv_file_name = os.path.join(output_dir, 'coc_results.csv')
        df = coc_results.to_frame()
        df.to_csv(csv_file_name, index=True)

        csv_file_name = os.path.join(output_dir, 'summary_results.csv')
        df = summary_results.to_frame().T
        df.sort_index(inplace=True)
        df.to_csv(csv_file_name, index=True, index_label='index', header=['value'])


@orca.step()
def print_results():

    print "---------- print_results"

    with orca.eval_variable('output_store_for_read') as hdf:

        for key in hdf.keys():

            print "\n========== %s\n" % key
            print hdf[key]
