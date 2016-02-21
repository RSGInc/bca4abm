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

parent_dir = os.path.dirname(__file__)
orca.add_injectable('configs_dir', os.path.join(parent_dir, 'configs'))
orca.add_injectable('data_dir', os.path.join(parent_dir, 'data'))
orca.add_injectable('output_dir', os.path.join(parent_dir, 'output'))


def test_read_persons_table():

    settings = orca.eval_variable('settings')
    assert settings.get('persons') == 'persons.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by persons_column_map values
    raw_persons = orca.get_table('raw_persons').to_frame()
    assert expect_columns(raw_persons,
                          settings['persons_column_map'].values())

    persons = orca.get_table('persons').to_frame()
    assert not missing_columns(persons,
                               settings['persons_column_map'].values())

    assert 'adult' in persons.columns
    assert 'coc_age' not in persons.columns


def test_read_households_table():

    settings = orca.eval_variable('settings')
    assert settings.get('households') == 'households.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by persons_column_map values
    raw_households = orca.get_table('raw_households').to_frame()
    assert expect_columns(raw_households,
                          settings['households_column_map'].values())

    households = orca.get_table('households').to_frame()
    assert not missing_columns(households,
                               settings['households_column_map'].values())


def test_persons_merged_table():

    settings = orca.eval_variable('settings')
    assert settings.get('store') is None

    persons_merged = orca.get_table('persons_merged').to_frame()
    assert 'person_gender' in persons_merged.columns
    assert 'hh_income' in persons_merged.columns

    # check for presence of computed column 'adult'
    assert 'adult' in persons_merged.columns

    # check that adult column is correctly computed
    assert (persons_merged.adult == (persons_merged.person_age > 18)).all()

    raw_persons = orca.get_table('raw_persons').to_frame()
    assert (persons_merged.person_type == raw_persons.person_type).all()


def test_read_trips_table():

    settings = orca.eval_variable('settings')
    assert settings.get('bca_base_trips') == 'basetrips.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by persons_column_map values
    raw_trips = orca.get_table('raw_trips').to_frame()
    raw_columns = settings['trips_column_map'].values() + ['build']
    assert expect_columns(raw_trips, raw_columns)

    trips = orca.get_table('trips').to_frame()
    assert not missing_columns(trips, raw_columns)


def test_read_trips_alt_table():

    settings = orca.eval_variable('settings')
    assert settings.get('bca_base_trips_alt') == 'basetrips_alt.csv'
    assert settings.get('store') is None

    # expect all of and only the columns specified by persons_column_map values
    raw_trips = orca.get_table('raw_trips_alt').to_frame()
    raw_columns = settings['trips_alt_column_map'].values() + ['build']
    assert expect_columns(raw_trips, raw_columns)

    trips = orca.get_table('trips_alt').to_frame()
    assert not missing_columns(trips, raw_columns)


def test_trips_merged_table():

    settings = orca.eval_variable('settings')
    assert settings.get('store') is None

    trips = orca.get_table('trips_merged').to_frame()
    assert 'auto_time' in trips.columns
    assert 'alt_auto_time' in trips.columns


def test_trips_with_demographics_table():

    settings = orca.eval_variable('settings')
    assert settings.get('store') is None

    trips = orca.get_table('trips_with_demographics').to_frame()
    assert 'auto_time' in trips.columns
    assert 'alt_auto_time' in trips.columns
    assert 'person_age' in trips.columns
    assert 'hh_income' in trips.columns

    # check for presence of computed column 'adult'
    assert 'adult' in trips.columns

    # check that adult column is correctly computed
    assert (trips.adult == (trips.person_age > 18)).all()
