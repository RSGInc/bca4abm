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


def test_run_4step():

    settings = inject_settings(
        chunk_size=None,
        trace_hh_id=None,
        trace_od=None)

    inject.clear_cache()

    tracing.config_logger()

    MODELS = settings['models']

    pipeline.run(models=MODELS, resume_after=None)

    pipeline.close_pipeline()


def test_zero_chunk_size():

    settings = inject_settings(chunk_size=0)

    inject.clear_cache()

    tracing.config_logger()

    MODELS = settings['models']

    pipeline.run(models=MODELS, resume_after='aggregate_od_processor')

    pipeline.close_pipeline()
