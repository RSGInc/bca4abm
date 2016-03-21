import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx

from bca4abm import bca4abm as bca
from ..util.misc import missing_columns, add_result_columns, add_summary_results

"""
Aggregate trips processor
"""


@orca.injectable()
def aggregate_trips_spec(configs_dir):
    f = os.path.join(configs_dir, 'aggregate_trips.csv')
    return bca.read_assignment_spec(f)


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


def scalar_assign_variables(assignment_expressions, locals_d):
    """
    Evaluate a set of variable expressions from a spec in the context
    of a given data table.

    Python expressions are evaluated in the context of this function using
    Python's eval function.
    Users should take care that these expressions must result in
    a scalar

    Parameters
    ----------
    exprs : sequence of str
    locals_d : Dict
        This is a dictionary of local variables that will be the environment
        for an evaluation of an expression that begins with @

    Returns
    -------
    variables : pandas.DataFrame
        Will have the index of `df` and columns of `exprs`.

    """

    # avoid trashing parameter when we add target
    locals_d = locals_d.copy() if locals_d is not None else {}

    l = []
    # need to be able to identify which variables causes an error, which keeps
    # this from being expressed more parsimoniously
    for e in zip(assignment_expressions.target, assignment_expressions.expression):
        target = e[0]
        expression = e[1]

        # print "\n%s = %s" % (target, expression)

        try:
            if expression.startswith('@'):
                expression = expression[1:]

            value = eval(expression, globals(), locals_d)

            # print "\n%s = %s" % (target, value)

            l.append((target, [value]))

            # FIXME - do we want to update locals to allows us to ref previously assigned targets?
            locals_d[target] = value
        except Exception as err:
            print "Variable %s expression failed for: %s" % (str(target), str(expression))
            raise err

    # since we allow targets to be recycled, we want to only keep the last usage
    keepers = []
    for statement in reversed(l):
        # don't keep targets that staert with underscore
        if statement[0].startswith('_'):
            continue
        # add statement to keepers list unless target is already in list
        if not next((True for keeper in keepers if keeper[0] == statement[0]), False):
            keepers.append(statement)

    return pd.DataFrame.from_items(keepers)


@orca.step()
def aggregate_trips_processor(aggregate_trips_manifest, aggregate_trips_spec, settings, data_dir):

    print "---------- aggregate_trips_processor"

    # print aggregate_trips_manifest

    assert not missing_columns(aggregate_trips_manifest,
                               settings['aggregate_data_manifest_column_map'].values())

    locals_d = settings['locals']
    if 'locals_aggregate_trips' in settings:
        locals_d.update(settings['locals_aggregate_trips'])

    results = None
    for row in aggregate_trips_manifest.itertuples(index=True):

        # print "   %s" % row.description

        matrix_dir = os.path.join(data_dir, "base-data")
        locals_d['base_trips'] = get_omx_matrix(matrix_dir, row.trip_file_name, row.trip_table_name)
        locals_d['base_ivt'] = get_omx_matrix(matrix_dir, row.ivt_file_name, row.ivt_table_name)
        locals_d['base_aoc'] = get_omx_matrix(matrix_dir, row.aoc_file_name, row.aoc_table_name)
        locals_d['base_toll'] = get_omx_matrix(matrix_dir, row.toll_file_name, row.toll_table_name)

        matrix_dir = os.path.join(data_dir, "build-data")
        locals_d['build_trips'] = get_omx_matrix(matrix_dir, row.trip_file_name,
                                                 row.trip_table_name)
        locals_d['build_ivt'] = get_omx_matrix(matrix_dir, row.ivt_file_name, row.ivt_table_name)
        locals_d['build_aoc'] = get_omx_matrix(matrix_dir, row.aoc_file_name, row.aoc_table_name)
        locals_d['build_toll'] = get_omx_matrix(matrix_dir, row.toll_file_name, row.toll_table_name)

        locals_d['aoc_units'] = row.aoc_units
        locals_d['toll_units'] = row.toll_units
        locals_d['vot'] = row.vot

        row_results = scalar_assign_variables(assignment_expressions=aggregate_trips_spec,
                                              locals_d=locals_d)

        assigned_column_names = row_results.columns.values
        row_results.insert(loc=0, column='description', value=row.description)
        row_results.insert(loc=0, column='manifest_idx', value=row.Index)

        if results is None:
            results = row_results
        else:
            results = results.append(row_results, ignore_index=True)

    results.reset_index(inplace=True)

    # print "\nassigned_column_names\n", assigned_column_names

    add_summary_results(results, summary_column_names=assigned_column_names,
                        prefix='AT_', spec=aggregate_trips_spec)

    with orca.eval_variable('output_store') as output_store:
        # for troubleshooting, write table with benefits for each row in manifest
        output_store['aggregate_trips'] = results

    if settings.get("dump", False) and settings.get("dump_aggregate_trips", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'aggregate_trips_benefits.csv')
        print "writing", csv_file_name
        results.to_csv(csv_file_name, index=False)
