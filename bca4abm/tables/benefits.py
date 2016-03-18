import os.path
import numpy as np
import orca
import pandas as pd


@orca.table(cache=True)
def summary_results():
    return pd.DataFrame()


@orca.table(cache=True)
def coc_results():
    return pd.DataFrame()


@orca.injectable()
def coc_column_names(coc_results):
    column_names = coc_results.index.names
    return column_names


@orca.injectable(cache=True)
def data_dictionary():
    return {}
