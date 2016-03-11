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

from bca4abm.util.misc import expect_columns, missing_columns, extra_columns, get_setting

parent_dir = os.path.dirname(__file__)
orca.add_injectable("configs_dir", os.path.join(parent_dir, 'configs'))
orca.add_injectable("data_dir", os.path.join(parent_dir, 'data'))
orca.add_injectable("output_dir", os.path.join(parent_dir, 'output'))


def test_initialize():

    orca.run(["initialize_output_store"])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert hdf.keys() == []


def test_print_results_processor(capsys):

    orca.run(["initialize_output_store"])
    orca.run(["demographics_processor"])
    orca.run(['write_results'])

    orca.run(['print_results'])
    out, err = capsys.readouterr()

    assert '/coc_results' in out
    assert '/summary_results' in out


def test_demographics_processor():

    persons_merged = orca.eval_variable('persons_merged').to_frame()
    assert "coc_age" not in persons_merged.columns

    orca.run(["initialize_output_store"])
    orca.run(["demographics_processor"])
    orca.run(['write_results'])

    persons_merged = orca.eval_variable('persons_merged').to_frame()
    assert "coc_age" in persons_merged.columns

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/summary_results' in hdf.keys()
        assert '/coc_results' in hdf.keys()
        npt.assert_equal(hdf['summary_results'].persons[0], 27)
        npt.assert_equal(hdf['coc_results'].persons.sum(), 27)


def test_person_trips_processor():

    orca.run(["initialize_output_store"])
    orca.run(["demographics_processor"])
    orca.run(["person_trips_processor"])
    orca.run(['write_results'])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/summary_results' in hdf.keys()
        assert '/coc_results' in hdf.keys()
        npt.assert_almost_equal(hdf['summary_results'].PT_total[0], 139004775.0)
        npt.assert_almost_equal(hdf['coc_results'].PT_total.sum(), 139004775.0)


def test_aggregate_trips_processor():

    orca.run(["initialize_output_store"])
    orca.run(["aggregate_trips_processor"])
    orca.run(['write_results'])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/aggregate_trips' in hdf.keys()
        assert '/summary_results' in hdf.keys()
        npt.assert_almost_equal(hdf['aggregate_trips'].total_benefit[0],
                                209733562.50, decimal=2)
        npt.assert_equal(hdf['aggregate_trips'].vot[0], 50.0)
        npt.assert_almost_equal(hdf['summary_results'].AT_ivt_benefit[0],
                                182495437.50, decimal=2)
        npt.assert_almost_equal(hdf['summary_results'].AT_aoc_benefit[0],
                                18249543.75, decimal=2)
        npt.assert_almost_equal(hdf['summary_results'].AT_toll_benefit[0],
                                8988581.25, decimal=2)
        npt.assert_almost_equal(hdf['summary_results'].AT_total_benefit[0],
                                209733562.50, decimal=2)


def test_link_daily_processor():

    orca.run(["initialize_output_store"])
    orca.run(["link_daily_processor"])
    orca.run(['write_results'])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/summary_results' in hdf.keys()
        npt.assert_almost_equal(hdf['summary_results'].LD_crash_cost_pdo[0],
                                -463938023.432, decimal=2)
        npt.assert_almost_equal(hdf['summary_results'].LD_crash_cost_total[0],
                                -1113451256.237, decimal=2)


def test_link_processor():

    orca.run(["initialize_output_store"])
    orca.run(["link_processor"])
    orca.run(['write_results'])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/link_results' in hdf.keys()
        npt.assert_almost_equal(hdf['link_results'].cost_op_total[0],
                                -79044707.159, decimal=2)
        npt.assert_almost_equal(hdf['link_results'].cost_delay_total[0],
                                12216076.978, decimal=2)
        assert '/summary_results' in hdf.keys()
        npt.assert_almost_equal(hdf['summary_results'].L_cost_op_total[0],
                                -79044707.159, decimal=2)
        npt.assert_almost_equal(hdf['summary_results'].L_cost_delay_total[0],
                                12216076.978, decimal=2)


def test_auto_ownership_processor():

    orca.run(["initialize_output_store"])
    orca.run(["demographics_processor"])
    orca.run(["auto_ownership_processor"])
    orca.run(['write_results'])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/summary_results' in hdf.keys()
        assert '/coc_results' in hdf.keys()
        npt.assert_almost_equal(hdf['summary_results'].AO_diff_auto_ownership_cost[0],
                                20000000.0, decimal=2)
        npt.assert_almost_equal(hdf['coc_results'].AO_diff_auto_ownership_cost.sum(),
                                20000000.0, decimal=2)


def test_physical_activity_processor():

    orca.run(["initialize_output_store"])
    orca.run(["demographics_processor"])
    orca.run(["physical_activity_processor"])
    orca.run(['write_results'])

    with orca.eval_variable('output_store_for_read') as hdf:
        assert '/summary_results' in hdf.keys()
        assert '/coc_results' in hdf.keys()
        npt.assert_almost_equal(hdf['summary_results'].PA_benefit_risk_reduction[0],
                                -37374708.6247, decimal=2)
        npt.assert_almost_equal(hdf['coc_results'].PA_benefit_risk_reduction.sum(),
                                -37374708.6247, decimal=2)
