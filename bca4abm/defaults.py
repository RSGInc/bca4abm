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
    if "output_store" in settings:
        return settings["output_store"]
    else:
        return 'bca_results.h5'


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
