import os.path

import pytest
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


@pytest.fixture(scope="module", autouse=True)
def inject_default_directories(request):

    parent_dir = os.path.dirname(__file__)
    orca.add_injectable("configs_dir", os.path.join(parent_dir, 'configs'))
    orca.add_injectable("data_dir", os.path.join(parent_dir, 'data'))
    orca.add_injectable("output_dir", os.path.join(parent_dir, 'output'))

    request.addfinalizer(orca.clear_cache)


def test_initialize():

    orca.run(["initialize_stores"])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == []


def test_aggregate_demographics_processor():

    # persons_merged = orca.eval_variable('persons_merged').to_frame()
    # assert "coc_age" not in persons_merged.columns

    orca.run(["initialize_stores"])
    orca.run(["aggregate_demographics_processor"])
    orca.run(['write_results'])

    # persons_merged = orca.eval_variable('persons_merged').to_frame()
    # assert "coc_age" in persons_merged.columns
    #
    # with orca.eval_variable('output_store_for_read') as hdf:
    #     assert '/summary_results' in hdf.keys()
    #     assert '/coc_results' in hdf.keys()
    #     npt.assert_equal(hdf['summary_results'].persons[0], 27)
    #     npt.assert_equal(hdf['coc_results'].persons.sum(), 27)
