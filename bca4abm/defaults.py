# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import os.path
import pandas as pd
import orca
import yaml


@orca.injectable()
def configs_dir():
    return 'configs'


@orca.injectable()
def data_dir():
    return 'data'


@orca.injectable()
def output_dir():
    return 'output'


@orca.injectable()
def settings_file_name():
    return 'settings.yaml'


@orca.injectable()
def settings(configs_dir, settings_file_name):
    with open(os.path.join(configs_dir, settings_file_name)) as f:
        return yaml.load(f)


@orca.injectable()
def output_store_file_name(settings):
    return settings.get("output_store", 'bca_results.h5')


@orca.injectable()
def output_store(output_dir, output_store_file_name):

    output_store_path = os.path.join(output_dir, output_store_file_name)

    print "opening output_store for UPDATE", output_store_path

    return pd.HDFStore(output_store_path)


@orca.injectable()
def output_store_for_read(output_dir, output_store_file_name):

    output_store_path = os.path.join(output_dir, output_store_file_name)

    print "opening output_store for READ", output_store_path

    return pd.HDFStore(output_store_path, mode='r')


@orca.injectable()
def input_source(settings):
    source = 'read_from_csv'
    # source = 'read_from_store'
    # source = 'update_store_from_csv'

    return settings.get("input_source", source)


@orca.injectable()
def input_store_file_name(settings):
    return settings.get("input_store", 'store.h5')


@orca.injectable()
def input_store_for_update(data_dir, input_store_file_name):

    input_store_path = os.path.join(data_dir, input_store_file_name)

    # print "opening input_store for UPDATE", input_store_path

    return pd.HDFStore(input_store_path)


@orca.injectable()
def input_store_for_read(data_dir, input_store_file_name):

    input_store_path = os.path.join(data_dir, input_store_file_name)

    # print "opening input_store for READ", input_store_path

    return pd.HDFStore(input_store_path, mode='r')


@orca.injectable()
def input_store_for_write(data_dir, input_store_file_name):

    input_store_path = os.path.join(data_dir, input_store_file_name)

    # print "opening input_store for READ", input_store_path

    return pd.HDFStore(input_store_path, mode='w')
