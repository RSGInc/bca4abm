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
def coc_column_names():

    raise RuntimeError("coc_column_names not initialized"
                       " - did you forget to run demographics_processor?")


@orca.injectable(cache=True)
def data_dictionary():
    return {}


@orca.table(cache=True)
def aggregate_results():
    return pd.DataFrame()


@orca.injectable(cache=True)
def coc_silos():

    raise RuntimeError("coc_silos not initialized"
                       " - did you forget to run aggregate_demographics_processor?")
