# bca4abm
# See full license in LICENSE.txt.

import logging

import os
import pandas as pd

from activitysim.core import config
from activitysim.core import inject
from activitysim.core import tracing
from activitysim.core import assign
from activitysim.core import pipeline

from bca4abm import bca4abm as bca
from ...util.misc import add_result_columns, add_summary_results

from activitysim.core.util import assign_in_place


logger = logging.getLogger(__name__)

"""
Demographics processor
"""


@inject.injectable()
def demographics_spec(configs_dir):
    f = os.path.join(configs_dir, "demographics.csv")
    return bca.read_assignment_spec(f)


@inject.injectable()
def demographics_settings(configs_dir):
    return config.read_model_settings(configs_dir, 'demographics.yaml')


@inject.step()
def demographics_processor(
        persons, persons_merged,
        demographics_spec, demographics_settings,
        chunk_size,
        trace_hh_id):

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    persons_df = persons_merged.to_frame()

    logger.info("Running demographics_processor with %d persons (chunk size = %s)"
                % (len(persons_df), chunk_size))

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = config.get_model_constants(demographics_settings)
    locals_dict.update(config.setting('globals'))

    trace_rows = trace_hh_id and persons_df['household_id'] == trace_hh_id

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    results, trace_results, trace_assigned_locals \
        = assign.assign_variables(demographics_spec,
                                  persons_df,
                                  locals_dict,
                                  df_alias='persons',
                                  trace_rows=trace_rows)

    # add assigned columns to persons as they are needed by downstream processors
    persons = persons.to_frame()
    assign_in_place(persons, results)
    pipeline.replace_table("persons", persons)

    # coc groups with counts
    # TODO - should we allow specifying which assigned columns are coc (e.g. in settings?)
    # for now, assume all assigned columns are coc, but this could cramp modelers style
    # if they want to create additional demographic columns for downstream use that aren't coc
    coc_columns = list(results.columns)

    inject.add_injectable("coc_column_names", coc_columns)

    # - create table with coc columns as indexes and a single column 'persons' with counts
    # index                        persons
    # coc_poverty coc_age
    # False       False            20
    #             True              3
    # True        False             4
    coc_grouped = results.groupby(coc_columns)
    coc_grouped = coc_grouped[coc_columns[0]].count().to_frame(name='persons')

    pipeline.replace_table("coc_results", coc_grouped)

    add_summary_results(coc_grouped)

    if trace_hh_id:

        if trace_results is not None:

            tracing.write_csv(trace_results,
                              file_name="demographics",
                              index_label='person_idx',
                              column_labels=['label', 'person'])

        if trace_assigned_locals:
            tracing.write_csv(trace_assigned_locals, file_name="demographics_locals")
