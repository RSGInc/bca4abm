import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx

from bca4abm import bca4abm as bca
from ..util.misc import missing_columns
from ..util.misc import add_summary_results
from ..util.misc import add_aggregate_results

from bca4abm import tracing

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
        df = bca.read_csv_or_tsv(fpath, header=0, usecols=usecols)
        df.rename(columns=column_map, inplace=True)
    else:
        df = bca.read_csv_or_tsv(fpath, header=0, comment='#')

    return df


@orca.injectable()
def link_spec(configs_dir):
    f = os.path.join(configs_dir, 'link.csv')
    return bca.read_assignment_spec(f)


@orca.injectable()
def link_daily_spec(configs_dir):
    f = os.path.join(configs_dir, 'link_daily.csv')
    return bca.read_assignment_spec(f)


def add_tables_to_locals(data_dir, settings, settings_tag, locals_dict):

    tables_tag = "tables_%s" % settings_tag
    if tables_tag in settings:

        file_list = settings.get(tables_tag)
        for var_name, filename in file_list.iteritems():

            # print "add_tables_to_locals %s = %s" % (var_name, filename)

            fpath = os.path.join(data_dir, filename)
            df = bca.read_csv_or_tsv(fpath, header=0, comment='#')

            locals_dict[var_name] = df

    return locals_dict


def eval_link_spec(link_spec, link_file_names, data_dir, link_file_column_map,
                   settings, settings_tag, trace_tag=None, trace_od=None):

    locals_dict = bca.assign_variables_locals(settings, settings_tag)

    locals_dict = add_tables_to_locals(data_dir, settings, settings_tag, locals_dict)

    results = {}

    for scenario in ['base', 'build']:

        link_data_subdir = 'base-data' if scenario == 'base' else 'build-data'

        for i in range(len(link_file_names)):
            if i == 0:
                links_df = read_csv_file(data_dir=os.path.join(data_dir, link_data_subdir),
                                         file_name=link_file_names[0],
                                         column_map=link_file_column_map)
            else:
                links_df_add = read_csv_file(data_dir=os.path.join(data_dir, link_data_subdir),
                                             file_name=link_file_names[i],
                                             column_map=link_file_column_map)
                link_index_fields = settings['link_daily_index_fields']
                links_df = links_df.set_index(link_index_fields, drop=False)
                links_df_add = links_df_add.set_index(link_index_fields, drop=False)
                suffix = "_" + link_file_names[i].replace(".csv", "")
                links_df = links_df.join(links_df_add, how="outer", rsuffix=suffix)

        if trace_od:
            od_column = settings.get('%s_od_column' % settings_tag, None)
            if od_column:
                o, d = trace_od
                trace_rows = (links_df[od_column] == o) | (links_df[od_column] == d)
            else:
                # just dump first row
                trace_rows = (links_df.index == 1)
        else:
            trace_rows = None

        summary, trace_results, trace_assigned_locals = \
            bca.eval_and_sum(link_spec,
                             links_df,
                             locals_dict,
                             df_alias='links',
                             chunk_size=0,
                             trace_rows=trace_rows)

        results[scenario] = pd.DataFrame(data=summary).T

        if trace_tag and trace_assigned_locals is not None:
            tracing.write_locals(trace_assigned_locals,
                                 file_name="%s_locals_%s" % (settings_tag, scenario))

        if trace_results is not None:
            tracing.write_csv(trace_results,
                              file_name="%s_results_%s" % (settings_tag, scenario),
                              index_label='index',
                              column_labels=['label', 'link'])

    results = results['build'] - results['base']

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
                                     settings.get('link_table_column_map', None),
                                     settings,
                                     settings_tag='link')

        assigned_column_names = row_results.columns.values
        row_results.insert(loc=0, column='description', value=row.description)
        row_results.insert(loc=0, column='manifest_idx', value=row.Index)

        if results is None:
            results = row_results
        else:
            results = results.append(row_results, ignore_index=True)

    results.reset_index(inplace=True)

    add_summary_results(results, summary_column_names=assigned_column_names,
                        prefix='L_', spec=link_spec)

    if settings.get("dump", False) and settings.get("dump_link", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'link_benefits.csv')
        results.to_csv(csv_file_name, index=False)


@orca.step()
def link_daily_processor(link_daily_spec, settings, data_dir, trace_od):

    print "---------- link_daily_processor"

    if 'link_daily_file_names' in settings:
        link_daily_file_names = settings['link_daily_file_names']
    else:
        link_daily_file_names = settings['link_daily_file_name']

    results = eval_link_spec(link_daily_spec,
                             link_daily_file_names,
                             data_dir,
                             settings.get('link_daily_table_column_map', None),
                             settings,
                             settings_tag='link_daily',
                             trace_tag='link_daily',
                             trace_od=trace_od)

    if 'silos' in link_daily_spec.columns:
        add_aggregate_results(results, link_daily_spec, source='link_daily', zonal=False)
    else:
        add_summary_results(results, prefix='LD_', spec=link_daily_spec)

    if settings.get("dump", False) and settings.get("dump_link_daily", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'link_daily_benefits.csv')
        results.to_csv(csv_file_name, index=False)
