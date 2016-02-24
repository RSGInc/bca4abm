import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx

from bca4abm import bca4abm as bca
from ..util.misc import missing_columns

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


def read_csv_file(data_dir, file_name, index_col=None, column_map=None):

    fpath = os.path.join(data_dir, file_name)

    if column_map:
        usecols = column_map.keys()
        # print "read_bca_table usecols: ", usecols
        # FIXME - should we allow comment lines?
        df = pd.read_csv(fpath, header=0, usecols=usecols)
        df.rename(columns=column_map, inplace=True)
    else:
        df = pd.read_csv(fpath, header=0, comment='#')

    if index_col is not None:
        if index_col in df.columns:
            df.set_index(index_col, inplace=True)
        else:
            df.index.names = [index_col]

    return df


@orca.injectable()
def link_spec(configs_dir):
    f = os.path.join(configs_dir, 'link.csv')
    return bca.read_assignment_spec(f)


@orca.injectable(cache=True)
def fuel_consumption(data_dir, settings):

    fuel_rate = read_csv_file(data_dir,
                              file_name=settings['fuel_consumption'],
                              index_col='speed',
                              column_map=settings['fuel_consumption_column_map'])

    low_speed = fuel_rate.index[0]
    top_speed = fuel_rate.index[-1]

    # links is in closure
    def f_car(mph):
        return mph.astype(int).clip(low_speed, top_speed).map(fuel_rate.car)

    def f_truck(mph):
        return mph.astype(int).clip(low_speed, top_speed).map(fuel_rate.truck)

    return f_car, f_truck


@orca.step()
def link_processor(link_manifest, link_spec, settings, data_dir):

    print "---------- aggregate_trips_processor"

    assert not missing_columns(link_manifest,
                               settings['link_data_manifest_column_map'].values())

    # print "\nlink_manifest\n", link_manifest
    # print "\nlink_spec\n", link_spec

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    fuel_consumption_car, fuel_consumption_truck = orca.eval_variable("fuel_consumption")
    locals_d = {
        'fuel_consumption_car': fuel_consumption_car,
        'fuel_consumption_truck': fuel_consumption_truck}
    if 'link_processor_locals' in settings:
        locals_d.update(settings['link_processor_locals'])

    results = []
    for row in link_manifest.itertuples(index=True):

        link_data_dir = os.path.join(data_dir, "base")

        links = read_csv_file(link_data_dir,
                              file_name=row.link_file_name,
                              column_map=settings['link_table_column_map'])

        # eval_variables evaluates each of the expressions in spec
        # in the context of each row in of the choosers dataframe
        results = bca.assign_variables(link_spec, links, locals_d.copy())

        print "\n### %s" % row.description
        print "\n### %s" % row.link_file_name
        # print "\n%s" % links[: 10]
        #
        print "\n### link_processor - results"
        print results[: 10]
