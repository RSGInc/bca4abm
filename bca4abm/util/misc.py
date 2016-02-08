import os.path
import pandas as pd


def read_bca_table(table_name, index_col, data_dir, settings):

    # settings:
    #   <table_name>: <csv fiel name>
    #   <table_name>_column_map: { 'csv_col_name' : table_col_name', ... }
    #
    if table_name not in settings:
        return None

    fpath = os.path.join(data_dir, 'data', settings[table_name])
    df = pd.read_csv(fpath)

    column_map_key = table_name + "_column_map"
    if column_map_key in settings:
        df.rename(columns=settings[column_map_key], inplace=True)

    if index_col in df.columns:
        df.set_index(index_col, inplace=True)
    else:
        df.index.names = [index_col]

    return df
