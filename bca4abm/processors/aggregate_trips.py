import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx

from bca4abm import bca4abm as bca
from ..util.misc import missing_columns

"""
Aggregate trips processor
"""


@orca.injectable()
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
    omx_file = omx.openFile(omx_file_name, 'r')
    matrix = omx_file[omx_key][:, :]
    if close_after_read:
        # print "closing %s / %s '%s'" % (matrix_dir, omx_file_name, omx_key)
        omx_file.close()
    return matrix


def monetary_scale(units):
    return 1.0 if units == 'dollars' else 100.0 if units == 'cents' else 0.0


@orca.step()
def aggregate_trips_processor(aggregate_trips_manifest, settings, data_dir):

    print "---------- aggregate_trips_processor"

    # print aggregate_trips_manifest

    assert not missing_columns(aggregate_trips_manifest,
                               settings['aggregate_data_manifest_column_map'].values())

    results = []
    for row in aggregate_trips_manifest.itertuples(index=True):

        print "   %s" % row.description

        matrix_dir = os.path.join(data_dir, "base-matrices")
        base_trips = get_omx_matrix(matrix_dir, row.trip_file_name, row.trip_table_name)
        base_ivt = get_omx_matrix(matrix_dir, row.ivt_file_name, row.ivt_table_name)
        base_aoc = get_omx_matrix(matrix_dir, row.aoc_file_name, row.aoc_table_name)
        base_toll = get_omx_matrix(matrix_dir, row.toll_file_name, row.toll_table_name)

        matrix_dir = os.path.join(data_dir, "build-matrices")
        build_trips = get_omx_matrix(matrix_dir, row.trip_file_name, row.trip_table_name)
        build_ivt = get_omx_matrix(matrix_dir, row.ivt_file_name, row.ivt_table_name)
        build_aoc = get_omx_matrix(matrix_dir, row.aoc_file_name, row.aoc_table_name)
        build_toll = get_omx_matrix(matrix_dir, row.toll_file_name, row.toll_table_name)

        aoc_scale = monetary_scale(row.aoc_units)
        toll_scale = monetary_scale(row.toll_units)
        vot = row.vot * 1.0

        tt = \
            (0.5 * (base_trips + build_trips) * (base_ivt-build_ivt)).sum()
        monetized_tt = \
            vot * tt
        aoc = \
            (0.5 * (base_trips + build_trips) * aoc_scale * (base_aoc-build_aoc)).sum()
        toll = \
            (0.5 * (base_trips + build_trips) * toll_scale * (base_toll-build_toll)).sum()

        total = monetized_tt + aoc + toll

        aggregate_trips_benefits = {
            'description': row.description,
            'tt': tt,
            'vot': vot,
            'monetized_tt': monetized_tt,
            'aoc': aoc,
            'toll': toll,
            'total': total,
        }

        results.append(aggregate_trips_benefits)

    # create dataframe with results
    aggregate_trips_benefits = pd.DataFrame(results)

    with orca.eval_variable('output_store') as output_store:
        output_store['aggregate_trips'] = aggregate_trips_benefits
