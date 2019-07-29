import os.path

import numpy.testing as npt
import pandas as pd
import pandas.util.testing as pdt
import pytest

from activitysim.core import inject


# inject injectables complicate matters because the decorators are executed at module load time
# and since py.test collects modules and loads them at the start of a run
# if a test method does something that has a lasting side-effect, then that side effect
# will carry over not just to subsequent test functions, but to subsequently called modules
# for instance, columns added with add_column will remain attached to inject tables
# pytest-xdist allows us to run py.test with the --boxed option which runs every function
# with a brand new python interpreter
# py.test --boxed --cov bca4abm

# Also note that the following import statement has the side-effect of registering injectables:
from bca4abm import bca4abm as bca

from bca4abm.util.misc import expect_columns, missing_columns, extra_columns, mapped_columns


@pytest.fixture(scope="module", autouse=True)
def inject_default_directories(request):

    parent_dir = os.path.dirname(__file__)
    inject.add_injectable("configs_dir", os.path.join(parent_dir, 'configs'))
    inject.add_injectable("data_dir", os.path.join(parent_dir, 'data'))
    inject.add_injectable("output_dir", os.path.join(parent_dir, 'output'))

    request.addfinalizer(inject.clear_cache)


def test_read_zone_hh_table():

    zones = inject.get_table('zone_hhs').to_frame()

    assert zones.shape[0] == 25


def test_read_zones_table():

    zones = inject.get_table('zones').to_frame()

    # number of zones in sample data
    assert zones.shape[0] == 25
