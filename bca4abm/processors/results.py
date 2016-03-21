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
def write_results(output_dir, coc_results, summary_results, settings):

    print "---------- write_results"

    with orca.eval_variable('output_store') as output_store:
        # reset multilevel index on coc_results (KISS)
        output_store['coc_results'] = coc_results.to_frame().reset_index(drop=False)
        output_store['summary_results'] = summary_results.to_frame()

    if settings.get("dump", False):

        data_dict = orca.get_injectable('data_dictionary')

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

            coc_silos.reset_index(inplace=True)
            coc_silos.rename(columns={'index': 'Target'}, inplace=True)
            # add the description from the data dictionary
            coc_silos['Description'] = coc_silos.Target.map(data_dict)

            csv_file_name = os.path.join(output_dir, 'coc_silos.csv')
            coc_silos.to_csv(csv_file_name, index=False)

        csv_file_name = os.path.join(output_dir, 'coc_results.csv')
        df = coc_results.to_frame()
        df.to_csv(csv_file_name, index=True)

        csv_file_name = os.path.join(output_dir, 'summary_results.csv')
        df = summary_results.to_frame().T
        df.sort_index(inplace=True)

        df.reset_index(inplace=True)
        df.columns = ['Target', 'Value']
        # add the description from the data dictionary
        df['Description'] = df.Target.map(data_dict)

        df.to_csv(csv_file_name, index=False)


@orca.step()
def print_results():

    print "---------- print_results"

    with orca.eval_variable('output_store_for_read') as hdf:

        for key in hdf.keys():

            print "\n========== %s\n" % key
            print hdf[key]
