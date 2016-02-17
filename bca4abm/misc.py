import os.path
import pandas as pd
import orca
import yaml

# import warnings
# warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
# pd.options.mode.chained_assignment = None


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


@orca.injectable(cache=True)
def output_store(output_dir, settings):
    if "output_store" in settings:
        return pd.HDFStore(os.path.join(output_dir, settings["output_store"]))
    else:
        return None


@orca.injectable(cache=True)
def store(data_dir, settings):
    if "store" in settings:
        return pd.HDFStore(os.path.join(data_dir, settings["store"]), mode='r')
    else:
        return None


@orca.step()
def write_output_store(output_store, settings):

    if output_store is not None:

        table_names = settings.get("output_store_tables", [])

        for table_name in table_names:
            print "   writing %s..." % table_name
            print "      columns: %s" % orca.eval_variable(table_name).to_frame().columns.values
            output_store[table_name] = orca.eval_variable(table_name).to_frame()
