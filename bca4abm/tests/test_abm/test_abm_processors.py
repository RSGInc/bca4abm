import os.path

import logging
import numpy.testing as npt
import pandas as pd

import pytest
import yaml

from activitysim.core import tracing
from activitysim.core import pipeline
from activitysim.core import inject


# Also note that the following import statement has the side-effect of registering injectables:
from bca4abm import bca4abm as bca


def teardown_function(func):
    inject.clear_cache()
    inject.reinject_decorated_tables()


def close_handlers():

    loggers = logging.Logger.manager.loggerDict
    for name in loggers:
        logger = logging.getLogger(name)
        logger.handlers = []
        logger.propagate = True
        logger.setLevel(logging.NOTSET)


def inject_settings(chunk_size=None, trace_hh_id=None, trace_od=None):

    configs_dir = os.path.join(os.path.dirname(__file__), 'configs')
    inject.add_injectable("configs_dir", configs_dir)

    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    inject.add_injectable("output_dir", output_dir)

    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    inject.add_injectable("data_dir", data_dir)

    with open(os.path.join(configs_dir, 'settings.yaml')) as f:
        settings = yaml.load(f)
        if chunk_size is not None:
            settings['chunk_size'] = chunk_size
        if trace_hh_id is not None:
            settings['trace_hh_id'] = trace_hh_id
        if trace_od is not None:
            settings['trace_od'] = trace_od

    inject.add_injectable("settings", settings)

    return settings


def run_abm(models, resume_after=None, chunk_size=None, trace_hh_id=None, trace_od=None):

    settings = inject_settings(
        chunk_size=chunk_size,
        trace_hh_id=trace_hh_id,
        trace_od=trace_od)

    inject.clear_cache()

    tracing.config_logger()

    pipeline.run(models=models, resume_after=resume_after)


def regress_abm():

    summary_results = pipeline.get_table("summary_results")
    coc_results = pipeline.get_table("coc_results")

    # demographics_processor
    npt.assert_equal(coc_results.persons.sum(), 27)

    def summary_value(summary_results, target):
        return summary_results[summary_results.Target == target].Value.iloc[0]

    npt.assert_equal(summary_value(summary_results, 'persons'), 27)

    # person_trips_processor
    npt.assert_equal(coc_results.PT_total.sum(), 139004775.0)
    npt.assert_equal(summary_value(summary_results, 'PT_total'), 139004775.0)

    # auto_ownership_processor
    npt.assert_almost_equal(coc_results.AO_diff_auto_ownership_cost.sum(),
                            20000000.0, decimal=2)
    npt.assert_almost_equal(summary_value(summary_results, 'AO_diff_auto_ownership_cost'),
                            20000000.0, decimal=2)

    # physical_activity_processor
    npt.assert_almost_equal(coc_results.PA_benefit_risk_reduction.sum(),
                            -37374708.6247, decimal=2)
    npt.assert_almost_equal(summary_value(summary_results, 'PA_benefit_risk_reduction'),
                            -37374708.6247, decimal=2)

    # aggregate_trips_processor
    npt.assert_equal(summary_value(summary_results, 'AT_vot'),
                     50.0)
    npt.assert_almost_equal(summary_value(summary_results, 'AT_ivt_benefit'),
                            182495437.50, decimal=2)
    npt.assert_almost_equal(summary_value(summary_results, 'AT_aoc_benefit'),
                            18249543.75, decimal=2)
    npt.assert_almost_equal(summary_value(summary_results, 'AT_toll_benefit'),
                            8988581.25, decimal=2)
    npt.assert_almost_equal(summary_value(summary_results, 'AT_total_benefit'),
                            209733562.50, decimal=2)

    # link_daily_processor
    npt.assert_almost_equal(summary_value(summary_results, 'LD_crash_cost_pdo'),
                            463938023.432, decimal=2)
    npt.assert_almost_equal(summary_value(summary_results, 'LD_crash_cost_total'),
                            1113451256.237, decimal=2)

    # link_processor
    npt.assert_almost_equal(summary_value(summary_results, 'L_cost_op_total'),
                            79044707.159, decimal=2)
    npt.assert_almost_equal(summary_value(summary_results, 'L_cost_delay_total'),
                            -12216076.978, decimal=2)


def test_full_run1():

    MODELS = [
        'demographics_processor',
        'person_trips_processor',
        'auto_ownership_processor',
        'physical_activity_processor',
        'aggregate_trips_processor',
        'link_daily_processor',
        'link_processor',
        'finalize_abm_results',
        'write_data_dictionary',
        'write_tables'
    ]

    # MODELS = setting('models')

    run_abm(MODELS, chunk_size=None, trace_hh_id=None, trace_od=None)
    regress_abm()

    pipeline.close_pipeline()


def test_full_run2():

    MODELS = [
        'demographics_processor',
        'person_trips_processor',
        'auto_ownership_processor',
        'physical_activity_processor',
        'aggregate_trips_processor',
        'link_daily_processor',
        'link_processor',
        'finalize_abm_results',
        'write_data_dictionary',
        'write_tables'
    ]

    # MODELS = setting('models')

    run_abm(MODELS, resume_after='physical_activity_processor',
            chunk_size=None, trace_hh_id=None, trace_od=None)

    regress_abm()

    pipeline.close_pipeline()


def test_chunked_full_run():
    MODELS = [
        'demographics_processor',
        'person_trips_processor',
        'auto_ownership_processor',
        'physical_activity_processor',
        'aggregate_trips_processor',
        'link_daily_processor',
        'link_processor',
        'finalize_abm_results',
        'write_data_dictionary',
        'write_tables'
    ]

    # MODELS = setting('models')

    run_abm(MODELS, resume_after='physical_activity_processor',
            chunk_size=2000, trace_hh_id=None, trace_od=None)

    regress_abm()

    pipeline.close_pipeline()
