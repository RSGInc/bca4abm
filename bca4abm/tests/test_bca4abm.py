import os.path

import numpy.testing as npt
import pandas as pd
import orca
import pandas.util.testing as pdt
import pytest

from activitysim import activitysim as asim

# this has the side-effect of registering ingectables
from activitysim import defaults

from .. tables import trips
from .. import bca4abm as bca

def test_read_settings():

    settings = orca.eval_variable('settings')
    assert settings.get('answer') == 42

