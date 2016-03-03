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

from bca4abm.processors.link import read_csv_file

parent_dir = os.path.dirname(__file__)
orca.add_injectable("configs_dir", os.path.join(parent_dir, 'configs'))
orca.add_injectable("data_dir", os.path.join(parent_dir, 'data'))
orca.add_injectable("output_dir", os.path.join(parent_dir, 'output'))


def test_initialize():

    orca.run(["initialize_output_store"])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == []


def test_demographics_processor():

    persons_merged = orca.eval_variable('persons_merged').to_frame()
    assert "coc_age" not in persons_merged.columns

    orca.run(["initialize_output_store"])
    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == []

    orca.run(["demographics_processor"])

    persons_merged = orca.eval_variable('persons_merged').to_frame()
    assert "coc_age" in persons_merged.columns

    # not sure what to test yet...


def test_person_trips_processor():

    orca.run(["initialize_output_store"])
    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == []

    orca.run(["demographics_processor"])
    orca.run(["person_trips_processor"])

    trips_with_demographics = \
        orca.eval_variable('trips_with_demographics').to_frame()

    assert "benefit" in trips_with_demographics.columns

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/person_trips' in hdf.keys()
        assert '/person_trips_coc' in hdf.keys()
        assert hdf['person_trips'].benefit[0] == 507780.0
        assert hdf['person_trips_coc'].benefit.sum() == 507780.0


def test_aggregate_trips_processor():

    orca.run(["initialize_output_store"])
    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == []

    orca.run(["aggregate_trips_processor"])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == ['/aggregate_trips']
        assert hdf['aggregate_trips'].monetized_tt_benefit[0] == 133330.0


def read_csv_file():

    fuel_rate = bca.read_csv_file(os.path.join(parent_dir, 'data'),
                                  file_name=get_setting('fuel_consumption'),
                                  index_col='speed',
                                  column_map=get_setting('fuel_consumption_column_map'))
    assert not missing_columns(fuel_rate, ['speed'])


def test_link_processor():

    orca.run(["initialize_output_store"])
    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == []

    orca.run(["link_processor"])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == ['/link']
        # TODO - should check values once we have them from test spec spreadsheet
