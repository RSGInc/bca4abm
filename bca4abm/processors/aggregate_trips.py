# bca4abm
# See full license in LICENSE.txt.

import logging
import os
import pandas as pd
import numpy as np
import openmatrix as omx

from activitysim.core import config
from activitysim.core import inject
from activitysim.core import tracing
from activitysim.core import pipeline

from bca4abm import bca4abm as bca
from ..util.misc import missing_columns, add_summary_results

logger = logging.getLogger(__name__)


"""
Aggregate trips processor
"""


@inject.injectable()
def aggregate_trips_spec():
    return bca.read_assignment_spec('aggregate_trips.csv')


@inject.injectable()
def aggregate_trips_settings():
    return config.read_model_settings('aggregate_trips.yaml')


@inject.injectable()
def aggregate_trips_manifest(data_dir, settings):
    fname = os.path.join(data_dir, 'aggregate_data_manifest.csv')

    # strings that might be empty and hence misconstrued as nans
    converters = {
        'toll_file_name': str,
        'toll_units': str,
    }
    manifest = pd.read_csv(fname, header=0, comment='#', converters=converters)

    column_map = "aggregate_data_manifest_column_map"

    if column_map in settings:
        usecols = settings[column_map].keys()
        manifest.rename(columns=settings[column_map], inplace=True)

    return manifest


def get_omx_matrix(matrix_dir, omx_file_name, omx_key, close_after_read=True):
    if not omx_file_name:
        return 0.0
    # print "reading %s / %s '%s'" % (matrix_dir, omx_file_name, omx_key)
    omx_file_name = os.path.join(matrix_dir, omx_file_name)
    omx_file = omx.open_file(omx_file_name, 'r')
    matrix = omx_file[omx_key][:, :]
    if close_after_read:
        # print "closing %s / %s '%s'" % (matrix_dir, omx_file_name, omx_key)
        omx_file.close()
    return matrix


@inject.step()
def aggregate_trips_processor(
        aggregate_trips_manifest,
        aggregate_trips_settings,
        aggregate_trips_spec,
        settings, data_dir):

    """
    Compute aggregate trips benefits

    The data manifest contains a list of trip count files (one for base, one for build)
    along with their their corresponding in-vehicle-time (ivt), operating cost (aoc),
    and toll skims.

    Since the skims are all aligned numpy arrays , we can express their benefit calculation as
    vector computations in the aggregate_trips_spec
    """

    logger.info("Running aggregate_trips_processor")

    assert not missing_columns(aggregate_trips_manifest,
                               settings['aggregate_data_manifest_column_map'].values())

    locals_dict = config.get_model_constants(aggregate_trips_settings)
    locals_dict.update(config.setting('globals'))

    results = None
    for row in aggregate_trips_manifest.itertuples(index=True):

        matrix_dir = os.path.join(data_dir, "base-data")
        locals_dict['base_trips'] = \
            get_omx_matrix(matrix_dir, row.trip_file_name, row.trip_table_name)
        locals_dict['base_ivt'] = \
            get_omx_matrix(matrix_dir, row.ivt_file_name, row.ivt_table_name)
        locals_dict['base_aoc'] = \
            get_omx_matrix(matrix_dir, row.aoc_file_name, row.aoc_table_name)
        locals_dict['base_toll'] = \
            get_omx_matrix(matrix_dir, row.toll_file_name, row.toll_table_name)

        matrix_dir = os.path.join(data_dir, "build-data")
        locals_dict['build_trips'] = \
            get_omx_matrix(matrix_dir, row.trip_file_name, row.trip_table_name)
        locals_dict['build_ivt'] = \
            get_omx_matrix(matrix_dir, row.ivt_file_name, row.ivt_table_name)
        locals_dict['build_aoc'] = \
            get_omx_matrix(matrix_dir, row.aoc_file_name, row.aoc_table_name)
        locals_dict['build_toll'] = \
            get_omx_matrix(matrix_dir, row.toll_file_name, row.toll_table_name)

        locals_dict['aoc_units'] = row.aoc_units
        locals_dict['toll_units'] = row.toll_units
        locals_dict['vot'] = row.vot

        row_results = bca.scalar_assign_variables(assignment_expressions=aggregate_trips_spec,
                                                  locals_dict=locals_dict)

        assigned_column_names = row_results.columns.values
        row_results.insert(loc=0, column='description', value=row.description)
        row_results.insert(loc=0, column='manifest_idx', value=row.Index)

        if results is None:
            results = row_results
        else:
            results = results.append(row_results, ignore_index=True)

    results.reset_index(inplace=True)

    add_summary_results(results, summary_column_names=assigned_column_names,
                        prefix='AT_', spec=aggregate_trips_spec)

    # for troubleshooting, write table with benefits for each row in manifest
    pipeline.replace_table("aggregate_trips_benefits", results)
