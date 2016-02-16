import os.path

import numpy.testing as npt
import pandas as pd
import orca
import pandas.util.testing as pdt
import pytest


# orca injectables complicate matters because the decorators are executed at module load time
# and since py.test collects modules and loads them at the start of a run
# if a test method does something that has a lasting side-effect, then that side effect
# will carry over not just to subsequent test functions, but to subsequently called modules
# for instance, columns added with add_column will remain attached to orca tables
# pytest-xdist allows us to run py.test with the --boxed option which runs every function
# with a brand new python interpreter
# py.test --boxed --cov bca4abm

# Also note that the following import statement has the side-effect of registering injectables:
from bca4abm import bca4abm as bca

from bca4abm.util.misc import expect_columns, missing_columns, extra_columns

orca.add_injectable("configs_dir", os.path.join(os.path.dirname(__file__)))
orca.add_injectable("data_dir", os.path.join(os.path.dirname(__file__)))
orca.add_injectable("output_dir", os.path.join(os.path.dirname(__file__)))


def test_read_settings():

    settings = orca.eval_variable('settings')
    assert settings.get('bca_households') == 'households.csv'
    assert settings.get('bca_persons') == 'persons.csv'
    assert settings.get('store')is None


def test_read_csv_table():

    data_dir = orca.eval_variable('data_dir')
    settings = {'bca_persons': 'persons.csv'}

    df = bca.read_csv_table("bca_persons", data_dir, settings, index_col='person_id')

    assert not missing_columns(df, ["pno", "hhno", "pptyp"])


def test_output_store():

    settings = orca.eval_variable('settings')
    settings["output_store"] = "store.h5"
    orca.add_injectable("settings", settings)

    # make sure we are reading from csv and not store
    assert orca.eval_variable('store') is None

    orca.run(["demographics_processor"])
    orca.run(["write_output_store"])

    output_store = orca.eval_variable('output_store')
    output_raw_bca_persons = output_store["raw_bca_persons"]

    raw_bca_persons = orca.eval_variable('raw_bca_persons').to_frame()

    assert "person_age" in output_raw_bca_persons.columns.values

    # expect all of and only the columns in bca_persons
    assert expect_columns(output_raw_bca_persons,
                          raw_bca_persons.columns.values)

    # expect all of and only the columns specified by bca_persons_column_map values
    assert expect_columns(output_raw_bca_persons,
                          settings["bca_persons_column_map"].values())


def test_store():

    orca.add_injectable("settings_file_name", "store_settings.yaml")

    settings = orca.eval_variable('settings')

    # expect all of and only the columns specified by bca_persons_column_map values
    raw_persons = orca.eval_variable('raw_bca_persons').to_frame()

    assert expect_columns(raw_persons,
                          settings["bca_persons_column_map"].values())

    persons = orca.eval_variable('bca_persons').to_frame()
    assert "person_gender" in persons.columns
    assert "adult" in persons.columns
    assert "coc_age" not in persons.columns.values
