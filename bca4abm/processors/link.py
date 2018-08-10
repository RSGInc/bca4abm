# bca4abm
# See full license in LICENSE.txt.

import logging

import os
import pandas as pd
import numpy as np
import openmatrix as omx

from activitysim.core import config
from activitysim.core import inject
from activitysim.core import tracing
from activitysim.core import assign

from bca4abm import bca4abm as bca
from ..util.misc import missing_columns
from ..util.misc import add_summary_results
from ..util.misc import add_aggregate_results


logger = logging.getLogger(__name__)


"""
Link processor
"""


@inject.injectable()
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


@inject.injectable()
def link_spec():
    return bca.read_assignment_spec('link.csv')


@inject.injectable()
def link_daily_spec():
    return bca.read_assignment_spec('link_daily.csv')


@inject.injectable()
def link_settings():
    return config.read_model_settings('link.yaml')


@inject.injectable()
def link_daily_settings():
    return config.read_model_settings('link_daily.yaml')


def add_tables_to_locals(data_dir, model_settings, locals_dict):

    tables_tag = "TABLES"
    if tables_tag in model_settings:

        file_list = model_settings.get(tables_tag)
        for var_name, filename in file_list.iteritems():

            # print "add_tables_to_locals %s = %s" % (var_name, filename)

            fpath = os.path.join(data_dir, filename)
            df = bca.read_csv_or_tsv(fpath, header=0, comment='#')

            locals_dict[var_name] = df

    return locals_dict


def eval_link_spec(link_spec, link_file_names, data_dir,
                   link_file_column_map, link_index_fields,
                   settings, model_settings, chunk_size, trace_tag=None, trace_od=None):

    # accept a single string as well as a dict of {suffix: filename}
    if isinstance(link_file_names, str):
        link_file_names = {"": link_file_names}

    locals_dict = config.get_model_constants(model_settings)
    locals_dict.update(config.setting('globals'))
    locals_dict = add_tables_to_locals(data_dir, model_settings, locals_dict)

    results = {}

    for scenario in ['base', 'build']:

        link_data_subdir = 'base-data' if scenario == 'base' else 'build-data'

        df_list = []
        for suffix, link_file_name in link_file_names.iteritems():

            df = read_csv_file(data_dir=os.path.join(data_dir, link_data_subdir),
                               file_name=link_file_name,
                               column_map=link_file_column_map)

            if link_index_fields:
                df.set_index(link_index_fields, drop=True, inplace=True)
            if suffix:
                df = df.add_suffix("_" + suffix)

            df_list.append(df)

        links_df = pd.concat(df_list, axis=1)

        # copy index fields into columns
        if link_index_fields:
            links_df = links_df.reset_index().set_index(link_index_fields, drop=False)

        if trace_od:
            od_column = model_settings.get('od_column', None)
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
                             chunk_size=chunk_size,
                             trace_rows=trace_rows)

        results[scenario] = summary

        if trace_results is not None:
            tracing.write_csv(trace_results,
                              file_name="%s_results_%s" % (trace_tag, scenario),
                              index_label='index',
                              column_labels=['label', 'link'])

            if trace_assigned_locals:
                tracing.write_csv(trace_assigned_locals,
                                  file_name="%s_locals_%s" % (trace_tag, scenario))

    results = results['build'] - results['base']

    results.reset_index(drop=True, inplace=True)

    return results


@inject.step()
def link_processor(
        link_manifest,
        link_spec,
        link_settings,
        settings, chunk_size, data_dir):

    assert not missing_columns(link_manifest,
                               settings['link_data_manifest_column_map'].values())

    results = None

    for row in link_manifest.itertuples(index=True):

        link_index_fields = None

        row_results = eval_link_spec(link_spec,
                                     row.link_file_name,
                                     data_dir,
                                     settings.get('link_table_column_map', None),
                                     link_index_fields,
                                     settings,
                                     model_settings=link_settings,
                                     chunk_size=chunk_size)

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


@inject.step()
def link_daily_processor(
        link_daily_spec,
        link_daily_settings,
        settings, chunk_size, data_dir, trace_od):

    if 'link_daily_file_names' in settings:
        link_daily_file_names = settings['link_daily_file_names']
    elif 'link_daily_file_name' in settings:
        link_daily_file_names = settings['link_daily_file_name']
    else:
        raise RuntimeError("no link_daily_file_names specified in settings file")

    results = eval_link_spec(link_daily_spec,
                             link_daily_file_names,
                             data_dir,
                             settings.get('link_daily_table_column_map', None),
                             settings.get('link_daily_index_fields', None),
                             settings,
                             model_settings=link_daily_settings,
                             chunk_size=chunk_size,
                             trace_tag='link_daily',
                             trace_od=trace_od)

    if 'silos' in link_daily_spec.columns:
        add_aggregate_results(results, link_daily_spec, source='link_daily', zonal=False)
    else:
        add_summary_results(results, prefix='LD_', spec=link_daily_spec)
