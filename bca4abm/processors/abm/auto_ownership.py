# bca4abm
# See full license in LICENSE.txt.

import logging

import os
import pandas as pd

from activitysim.core import config
from activitysim.core import inject
from activitysim.core import tracing
from activitysim.core import assign

from bca4abm import bca4abm as bca
from ...util.misc import add_result_columns, add_summary_results

logger = logging.getLogger(__name__)

"""
auto ownership processor
"""


@inject.injectable()
def auto_ownership_spec(configs_dir):
    f = os.path.join(configs_dir, "auto_ownership.csv")
    return bca.read_assignment_spec(f)


@inject.injectable()
def auto_ownership_settings(configs_dir):
    return config.read_model_settings(configs_dir, 'auto_ownership.yaml')


@inject.step()
def auto_ownership_processor(
        persons_merged,
        auto_ownership_spec,
        auto_ownership_settings,
        coc_column_names,
        chunk_size,
        trace_hh_id):

    """
    Compute auto ownership benefits
    """

    persons_df = persons_merged.to_frame()

    logger.info("Running auto_ownership_processor with %d persons (chunk size = %s)"
                % (len(persons_df), chunk_size))

    locals_dict = config.get_model_constants(auto_ownership_settings)
    locals_dict.update(config.setting('globals'))

    trace_rows = trace_hh_id and persons_df['household_id'] == trace_hh_id

    coc_summary, trace_results, trace_assigned_locals = \
        bca.eval_group_and_sum(assignment_expressions=auto_ownership_spec,
                               df=persons_df,
                               locals_dict=locals_dict,
                               df_alias='persons',
                               group_by_column_names=coc_column_names,
                               chunk_size=chunk_size,
                               trace_rows=trace_rows)

    result_prefix = 'AO_'
    add_result_columns("coc_results", coc_summary, result_prefix)
    add_summary_results(coc_summary, prefix=result_prefix, spec=auto_ownership_spec)

    if trace_hh_id:

        if trace_results is not None:

            tracing.write_csv(trace_results,
                              file_name="auto_ownership",
                              index_label='person_id',
                              column_labels=['label', 'person'])

        if trace_assigned_locals:
            tracing.write_csv(trace_assigned_locals, file_name="auto_ownership_locals")
