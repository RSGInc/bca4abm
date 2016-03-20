import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx

from bca4abm import bca4abm as bca
from ..util.misc import missing_columns
from ..util.misc import add_summary_results, add_result_columns

"""
Link processor
"""


@orca.injectable()
def link_manifest(data_dir, settings):
    fname = os.path.join(data_dir, 'link_data_manifest.csv')

    # strings that might be empty and hence misconstrued as nans
    converters = {
        # 'toll_file_name': str,
        # 'toll_units': str,
    }
    manifest = pd.read_csv(fname, header=0, comment='#', converters=converters)

    column_map = "link_data_manifest_column_map"

    if column_map in settings:
        usecols = settings[column_map].keys()
        manifest.rename(columns=settings[column_map], inplace=True)

    return manifest


def read_csv_file(data_dir, file_name, column_map=None):

    fpath = os.path.join(data_dir, file_name)

    if column_map:
        usecols = column_map.keys()
        # print "read_bca_table usecols: ", usecols
        # FIXME - should we allow comment lines?
        df = pd.read_csv(fpath, header=0, usecols=usecols)
        df.rename(columns=column_map, inplace=True)
    else:
        df = pd.read_csv(fpath, header=0, comment='#')

    return df


@orca.injectable()
def link_spec(configs_dir):
    f = os.path.join(configs_dir, 'link.csv')
    return bca.read_assignment_spec(f)


@orca.injectable()
def link_daily_spec(configs_dir):
    f = os.path.join(configs_dir, 'link_daily.csv')
    return bca.read_assignment_spec(f)


def eval_link_spec(link_spec, link_file_name, data_dir, link_file_column_map,
                   settings, settings_locals):

    locals_d = bca.assign_variables_locals(settings, settings_locals)

    results = {}

    for scenario in ['base', 'build']:

        link_data_subdir = 'base-matrices' if scenario == 'base' else 'build-matrices'

        links_df = read_csv_file(data_dir=os.path.join(data_dir, link_data_subdir),
                                 file_name=link_file_name,
                                 column_map=link_file_column_map)

        locals_d['links'] = links_df

        # eval_variables evaluates each of the expressions in spec
        # in the context of each row in of the choosers dataframe
        assigned_columns = bca.assign_variables(assignment_expressions=link_spec,
                                                df=links_df,
                                                locals_d=locals_d.copy())

        # print assigned_columns[:10]

        result = pd.DataFrame(data=assigned_columns.sum()).T

        results[scenario] = result

    results = results['base'] - results['build']

    results.reset_index(drop=True, inplace=True)

    return results


@orca.step()
def link_processor(link_manifest, link_spec, settings, data_dir):

    print "---------- link_processor"

    assert not missing_columns(link_manifest,
                               settings['link_data_manifest_column_map'].values())

    results = None

    for row in link_manifest.itertuples(index=True):

        row_results = eval_link_spec(link_spec,
                                     row.link_file_name,
                                     data_dir,
                                     settings['link_table_column_map'],
                                     settings,
                                     settings_locals='locals_link')

        assigned_column_names = row_results.columns.values
        row_results.insert(loc=0, column='description', value=row.description)
        row_results.insert(loc=0, column='manifest_idx', value=row.Index)

        if results is None:
            results = row_results
        else:
            results = results.append(row_results, ignore_index=True)

    results.reset_index(inplace=True)

    # print "\nassigned_column_names\n", assigned_column_names

    add_summary_results(results, summary_column_names=assigned_column_names,
                        prefix='L_', spec=link_spec)

    if settings.get("dump", False) and settings.get("dump_link", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'link_benefits.csv')
        print "writing", csv_file_name
        results.to_csv(csv_file_name, index=False)


@orca.step()
def link_daily_processor(link_daily_spec, settings, data_dir):

    print "---------- link_daily_processor"

    link_daily_file_name = settings['link_daily_file_name']

    results = eval_link_spec(link_daily_spec,
                             link_daily_file_name,
                             data_dir,
                             settings['link_table_column_map'],
                             settings,
                             settings_locals='locals_link_daily')

    add_summary_results(results, prefix='LD_', spec=link_daily_spec)

    if settings.get("dump", False) and settings.get("dump_link_daily", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'link_daily_benefits.csv')
        print "writing", csv_file_name
        results.to_csv(csv_file_name, index=False)
