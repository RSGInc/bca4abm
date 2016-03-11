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

from bca4abm.util.misc import expect_columns, missing_columns, extra_columns, get_setting


def test_misc():

    # expect all of and only the columns specified by persons_column_map values
    df = pd.DataFrame.from_items([('A', [1, 2, 3]), ('B', [4, 5, 6])])
    assert expect_columns(df, ['A', 'B'])

    with pytest.raises(Exception):
        expect_columns(df, ['A'])

    with pytest.raises(Exception):
        expect_columns(df, ['A', 'B', 'C'])

    assert get_setting('scenario_year') == 'sample'


def test_defaults():

    assert orca.eval_variable('settings_file_name') == 'settings.yaml'
    assert orca.eval_variable('output_store_file_name',
                              settings={}) == 'bca_results.h5'
    assert orca.eval_variable('output_store_file_name',
                              settings={'output_store': 'zorg.h5'}) == 'zorg.h5'


def test_read_settings():

    settings = orca.eval_variable('settings')
    assert settings.get('persons') == 'persons.csv'
    assert settings.get('store')is None


def test_read_csv_table():

    settings = {'persons': 'persons.csv'}
    data_dir = orca.eval_variable('data_dir')

    df = bca.read_csv_table(data_dir, settings, table_name="persons",  index_col='person_id')

    assert not missing_columns(df, ["pno", "hhno", "pptyp"])
