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

        # --------- alternate calculations

        # by base vs build change in generalized costs
        base_delta_cost = \
            (base_trips *
             ((build_ivt - base_ivt) * (vot / 60.0) +
              aoc_scale * (build_aoc - base_aoc) +
              toll_scale * (build_toll - base_toll)
              )).sum()
        build_delta_cost = \
            (build_trips *
             ((build_ivt - base_ivt) * (vot/60.0) +
              aoc_scale * (build_aoc - base_aoc) +
              toll_scale * (build_toll - base_toll)
              )).sum()
        generalized_cost_benefit = -0.5 * (base_delta_cost + build_delta_cost)

        # broken down by benefit category
        tt_benefit_in_minutes = \
            0.5 * ((base_trips + build_trips) * (base_ivt-build_ivt)).sum()
        monetized_tt_benefit = \
            vot/60.0 * tt_benefit_in_minutes
        aoc_benefit = \
            0.5 * ((base_trips + build_trips) * aoc_scale * (base_aoc-build_aoc)).sum()
        toll_benefit = \
            0.5 * ((base_trips + build_trips) * toll_scale * (base_toll-build_toll)).sum()
        categorized_benefit = monetized_tt_benefit + aoc_benefit + toll_benefit

        # omnibus (fastest)
        omnibus_benefit = \
            -0.5 * ((base_trips + build_trips) *
                    ((build_ivt - base_ivt) * (vot / 60.0) +
                     aoc_scale * (build_aoc - base_aoc) +
                     toll_scale * (build_toll - base_toll)
                     )).sum()

        aggregate_trips_benefits = {
            'description': row.description,

            'tt_benefit_in_minutes': tt_benefit_in_minutes,
            'vot': vot,
            'monetized_tt_benefit': monetized_tt_benefit,
            'aoc_benefit': aoc_benefit,
            'toll_benefit': toll_benefit,
            'categorized_benefit': categorized_benefit,

            # base vs build change in generalized costs
            'base_delta_cost': base_delta_cost,
            'build_delta_cost': build_delta_cost,
            'generalized_cost_benefit': generalized_cost_benefit,

            'benefit': omnibus_benefit
        }

        results.append(aggregate_trips_benefits)

    # create dataframe with results
    columns = ['description',
               'tt_benefit_in_minutes',
               'vot',
               'monetized_tt_benefit',
               'aoc_benefit',
               'toll_benefit',
               'categorized_benefit',
               'base_delta_cost',
               'build_delta_cost',
               'generalized_cost_benefit',
               'benefit'
               ]

    aggregate_trips_benefits = pd.DataFrame(results, columns=columns)

    with orca.eval_variable('output_store') as output_store:
        output_store['aggregate_trips'] = aggregate_trips_benefits
