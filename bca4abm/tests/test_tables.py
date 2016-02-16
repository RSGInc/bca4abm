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


def test_read_persons_table():

    settings = orca.eval_variable('settings')
    assert settings.get('bca_persons') == 'persons.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by bca_persons_column_map values
    raw_persons = orca.eval_variable('raw_bca_persons').to_frame()
    assert expect_columns(raw_persons,
                          settings["bca_persons_column_map"].values())

    persons = orca.eval_variable('bca_persons').to_frame()
    assert not missing_columns(persons,
                               settings["bca_persons_column_map"].values())

    assert "adult" in persons.columns
    assert "coc_age" not in persons.columns


def test_stored_persons_table():

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


def test_read_households_table():

    settings = orca.eval_variable('settings')
    assert settings.get('bca_households') == 'households.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by bca_persons_column_map values
    raw_households = orca.eval_variable('raw_bca_households').to_frame()
    assert expect_columns(raw_households,
                          settings["bca_households_column_map"].values())

    households = orca.eval_variable('bca_households').to_frame()
    assert not missing_columns(households,
                               settings["bca_households_column_map"].values())


def test_stored_households_table():

    orca.add_injectable("settings_file_name", "store_settings.yaml")

    settings = orca.eval_variable('settings')

    # expect all of and only the columns specified by bca_persons_column_map values
    raw_households = orca.eval_variable('raw_bca_households').to_frame()

    assert expect_columns(raw_households,
                          settings["bca_households_column_map"].values())


def test_persons_merged_table():

    settings = orca.eval_variable('settings')
    assert settings.get('store') is None

    persons_merged = orca.eval_variable('bca_persons_merged').to_frame()
    assert "person_gender" in persons_merged.columns
    assert "hh_income" in persons_merged.columns

    # check for presence of computed column 'adult'
    assert "adult" in persons_merged.columns

    # check that adult column is correctly computed
    assert (persons_merged.adult == (persons_merged.person_age > 18)).all()

    raw_persons = orca.eval_variable('raw_bca_persons').to_frame()
    assert (persons_merged.person_type == raw_persons.person_type).all()


def test_read_base_trips_table():

    settings = orca.eval_variable('settings')
    assert settings.get('bca_base_trips') == 'base_trips.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by bca_persons_column_map values
    raw_trips = orca.eval_variable('raw_bca_base_trips').to_frame()
    assert expect_columns(raw_trips,
                          settings["bca_trips_column_map"].values())

    trips = orca.eval_variable('bca_base_trips').to_frame()
    assert not missing_columns(trips,
                               settings["bca_trips_column_map"].values())


def test_read_base_trips_alt_table():

    settings = orca.eval_variable('settings')
    assert settings.get('bca_base_trips_alt') == 'base_trips_alt.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by bca_persons_column_map values
    raw_trips = orca.eval_variable('raw_bca_base_trips_alt').to_frame()
    assert expect_columns(raw_trips,
                          settings["bca_trips_alt_column_map"].values())

    trips = orca.eval_variable('bca_base_trips_alt').to_frame()
    assert not missing_columns(trips,
                               settings["bca_trips_alt_column_map"].values())


def test_base_trips_merged_table():

    settings = orca.eval_variable('settings')
    assert settings.get('store') is None

    trips = orca.eval_variable('bca_base_trips_merged').to_frame()
    assert "auto_time" in trips.columns
    assert "alt_auto_time" in trips.columns


def test_base_trips_with_demographics_table():

    settings = orca.eval_variable('settings')
    assert settings.get('store') is None

    trips = orca.eval_variable('bca_base_trips_with_demographics').to_frame()
    assert "auto_time" in trips.columns
    assert "alt_auto_time" in trips.columns
    assert "person_age" in trips.columns
    assert "hh_income" in trips.columns

    # check for presence of computed column 'adult'
    assert "adult" in trips.columns

    # check that adult column is correctly computed
    assert (trips.adult == (trips.person_age > 18)).all()
