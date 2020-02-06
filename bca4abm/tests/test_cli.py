import pytest
import subprocess

# the following import has the side-effect of registering injectables
from bca4abm import bca4abm as bca


def test_cli():

    # cp = completed process
    cp = subprocess.run(['bca4abm', '-h'], capture_output=True)

    assert 'usage: bca4abm [-h] [--version]' in str(cp.stdout)
