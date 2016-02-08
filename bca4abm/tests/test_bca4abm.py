import os.path

import numpy.testing as npt
import pandas as pd
import orca
import pandas.util.testing as pdt
import pytest

from activitysim import activitysim as asim

# this has the side-effect of registering injectables
from activitysim import defaults

from .. tables import trips
from .. import bca4abm as bca


def test_read_settings():

    orca.add_injectable("configs_dir", os.path.join(os.path.dirname(__file__)))

    settings = orca.eval_variable('settings')
    assert settings.get('bca_households') == 'households.csv'
    assert settings.get('bca_persons') == 'persons.csv'
    assert settings.get('store')is None


def test_read_persons_table():

    orca.add_injectable("configs_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("data_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("store", None)

    settings = orca.eval_variable('settings')
    assert settings.get('bca_persons') == 'persons.csv'
    assert settings.get('store') is None

    persons = orca.eval_variable('bca_persons').to_frame()
    assert "person_idx" in persons.columns


def test_merged_persons_table():

    orca.add_injectable("configs_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("data_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("store", None)

    settings = orca.eval_variable('settings')
    assert settings.get('bca_persons') == 'persons.csv'
    assert settings.get('store') is None

    persons_merged = orca.eval_variable('bca_persons_merged').to_frame()
    assert "person_gender" in persons_merged.columns
    assert "hh_income" in persons_merged.columns


def test_merged_persons_table():

    orca.add_injectable("configs_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("data_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("store", None)

    settings = orca.eval_variable('settings')
    assert settings.get('bca_persons') == 'persons.csv'
    assert settings.get('store') is None

    persons_merged = orca.eval_variable('bca_persons_merged').to_frame()
    assert "person_gender" in persons_merged.columns
    assert "hh_income" in persons_merged.columns


def test_demographics_processor():

    orca.add_injectable("configs_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("data_dir", os.path.join(os.path.dirname(__file__)))
    orca.add_injectable("store", None)

    orca.run(["demographics_processor"])

    # not sure what to test yet...
