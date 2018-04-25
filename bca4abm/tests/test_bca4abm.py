import os.path

import numpy.testing as npt
import pandas as pd
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


@pytest.fixture(scope="module", autouse=True)
def inject_default_directories(request):

    parent_dir = os.path.dirname(__file__)
    orca.add_injectable("configs_dir", os.path.join(parent_dir, 'configs'))
    orca.add_injectable("data_dir", os.path.join(parent_dir, 'data'))
    orca.add_injectable("output_dir", os.path.join(parent_dir, 'output'))

    request.addfinalizer(orca.clear_cache)


def test_read_settings():

    settings = orca.eval_variable('settings')

    assert settings.get('provenance') == 'tests.configs'
    assert settings.get('test_setting') == 'ping'
    assert settings.get('missing_setting') is None


def test_misc():

    # expect all of and only the columns specified by persons_column_map values
    df = pd.DataFrame.from_items([('A', [1, 2, 3]), ('B', [4, 5, 6])])
    assert expect_columns(df, ['A', 'B'])

    with pytest.raises(Exception):
        expect_columns(df, ['A'])

    with pytest.raises(Exception):
        expect_columns(df, ['A', 'B', 'C'])


def test_defaults():

    assert orca.eval_variable('settings_file_name') == 'settings.yaml'
    assert orca.eval_variable('output_store_file_name',
                              settings={}) == 'bca_results.h5'
    assert orca.eval_variable('output_store_file_name',
                              settings={'output_store': 'zorg.h5'}) == 'zorg.h5'


def test_read_csv_table():

    settings = {'persons': 'persons.csv'}
    data_dir = orca.eval_variable('data_dir')

    df = bca.read_csv_table(data_dir, settings, table_name="persons",  index_col='person_id')

    assert not missing_columns(df, ["pno", "hhno", "pptyp"])


def test_read_csv_table_with_tsv():

    settings = {'persons': 'persons.tsv'}
    data_dir = orca.eval_variable('data_dir')

    df = bca.read_csv_table(data_dir, settings, table_name="persons",  index_col='person_id')

    assert not missing_columns(df, ["pno", "hhno", "pptyp"])


def test_read_csv_table_with_txt():

    settings = {'persons': 'persons.txt'}
    data_dir = orca.eval_variable('data_dir')

    df = bca.read_csv_table(data_dir, settings, table_name="persons",  index_col='person_id')

    assert not missing_columns(df, ["pno", "hhno", "pptyp"])
