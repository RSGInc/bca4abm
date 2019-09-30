from __future__ import print_function
# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

from builtins import zip
from builtins import str
import logging

import pandas as pd
import os
from collections import OrderedDict

from activitysim.core import inject
from activitysim.core import assign
from activitysim.core import chunk


logger = logging.getLogger(__name__)


def read_csv_or_tsv(fpath, header='infer', usecols=None, comment=None):

    if fpath.endswith('.tsv'):
        sep = '\t'
    elif fpath.endswith('.txt'):
        sep = '\\s+'
    else:
        sep = ','

    try:
        return pd.read_csv(fpath, sep=sep, header=header, usecols=usecols, comment=comment)
    except UnicodeDecodeError:
        logger.warning(
            "Reading %s with default utf-8 encoding failed, trying cp1252 instead", fpath)
        return pd.read_csv(fpath, sep=sep, header=header, usecols=usecols, comment=comment,
                           encoding='cp1252')


def read_csv_table(data_dir, settings, table_name, index_col=None):

    # settings:
    #   <table_name>: <csv file name>
    #   <table_name>_column_map: { 'csv_col_name' : table_col_name', ... }
    #
    # settings = orca.eval_variable('settings')
    # data_dir = orca.eval_variable('data_dir')

    if table_name not in settings:
        return None

    fpath = os.path.join(data_dir, settings[table_name])

    column_map = table_name + "_column_map"

    if column_map in settings:
        usecols = list(settings[column_map].keys())
        # print "read_bca_table usecols: ", usecols
        # FIXME - should we allow comment lines?
        df = read_csv_or_tsv(fpath, header=0, usecols=usecols, comment='#')
        df.rename(columns=settings[column_map], inplace=True)
    else:
        df = read_csv_or_tsv(fpath, header=0, comment='#')

    if index_col is not None:
        if index_col in df.columns:
            df.set_index(index_col, inplace=True)
        else:
            df.index.names = [index_col]

    return df


def read_assignment_spec(fname):
    """
    Read a CSV model specification into a Pandas DataFrame or Series.

    The CSV is expected to have columns for component descriptions
    targets, and expressions,

    The CSV is required to have a header with column names. For example:

        Description,Target,Expression,Silos

    Parameters
    ----------
    fname : str
        Name of a CSV spec file.

    Returns
    -------
    spec : pandas.DataFrame
        The description column is dropped from the returned data and the
        expression values are set as the table index.
    """

    configs_dir = inject.get_injectable('configs_dir')
    fpath = os.path.join(configs_dir, fname)

    cfg = read_csv_or_tsv(fpath, comment='#')

    # drop null expressions
    # cfg = cfg.dropna(subset=[expression_name])

    # map column names to lower case
    cfg.columns = [x.lower() for x in cfg.columns]

    # backfill description
    if 'description' not in cfg.columns:
        cfg['description'] = ''

    cfg.target = cfg.target.str.strip()
    cfg.expression = cfg.expression.str.strip()

    if 'silos' in cfg.columns:
        cfg.silos.fillna('', inplace=True)
        cfg.silos = cfg.silos.str.strip()

    return cfg


# generator for chunked iteration over dataframe by chunk_size
def chunked_df(df, rows_per_chunk, trace_rows=None):

    logger.debug("rows_per_chunk ", rows_per_chunk)

    assert df.shape[0] > 0

    # generator to iterate over choosers in chunk_size chunks
    num_df_rows = len(df.index)
    num_chunks = (num_df_rows // rows_per_chunk) + (num_df_rows % rows_per_chunk > 0)

    i = offset = 0
    while offset < num_df_rows:
        if trace_rows is None:
            yield i + 1, num_chunks, df.iloc[offset: offset + rows_per_chunk], None
        else:
            yield i + 1, num_chunks, df.iloc[offset: offset + rows_per_chunk], \
                  trace_rows.iloc[offset: offset + rows_per_chunk]
        offset += rows_per_chunk
        i += 1


def chunked_df_by_chunk_id(df, trace_rows, rows_per_chunk, chunk_id_col='chunk_id'):
    # generator to iterate over df in chunk_size chunks
    # like chunked_df but based on chunk_id field rather than dataframe length
    # (the presumption is that df has multiple rows with the same chunk_id that
    # all have to be included in the same chunk)

    assert df.shape[0] > 0

    num_rows = df[chunk_id_col].max() + 1
    num_chunks = (num_rows // rows_per_chunk) + (num_rows % rows_per_chunk > 0)

    i = offset = 0
    while offset < num_rows:
        chunk_me = df[chunk_id_col].between(offset, offset + rows_per_chunk - 1)
        if trace_rows is None:
            yield i + 1, num_chunks, df[chunk_me], None
        else:
            yield i + 1, num_chunks, df[chunk_me], trace_rows[chunk_me]
        offset += rows_per_chunk
        i += 1


def calc_rows_per_chunk(chunk_size, df, spec, extra_columns=0, trace_label=None):
    """simple rows_per_chunk calculator for chunking calls to assign_variables

    ActivitySim's chunk.rows_per_chunk method handles the main logic, including
    a missing/zero chunk size

    Parameters
    ----------
    chunk_size : int
    df : pandas DataFrame
    spec : pandas DataFrame
    extra_columns : int, optional
    trace_label : str, optional

    Returns
    -------
    num_rows : int
    effective_chunk_size : int
    """

    num_rows = len(df.index)

    df_row_size = len(df.columns)

    # spec temp vars are transient and (we assume) discarded before extra_columns are applied
    # so the extra_columns headroom will be the max of the two
    spec_temps = spec.target.str.match('_').sum()
    spec_vars = spec.shape[0] - spec_temps
    row_size = df_row_size + spec_vars + max(spec_temps, extra_columns)

    # if trace_label:
    #     logger.debug("%s #chunk_calc df %s" % (trace_label, df.shape))
    #     logger.debug("%s #chunk_calc spec %s" % (trace_label, spec.shape))
    #     logger.debug("%s #chunk_calc extra_columns %s" % (trace_label, extra_columns))
    #     logger.debug("%s #chunk_calc row_size %s" % (trace_label, row_size))

    return chunk.rows_per_chunk(chunk_size, row_size, num_rows, trace_label)


def eval_and_sum(assignment_expressions, df, locals_dict,
                 group_by_column_names=None,
                 df_alias=None, chunk_size=0, trace_rows=None):
    """
    Evaluate assignment_expressions against df, and sum the results
    (sum by group if list of group_by_column_names is specified.
    e.g. group by coc column names and return sums grouped by community of concern.)

    Parameters
    ----------
    assignment_expressions
    df
    locals_dict
    group_by_column_names : array of str
        list of names of the columns to group by (e.g. coc_column_names of trip_coc_end)
    df_alias : str
        assign_variables df_alias (name of df in assignment_expressions)
    chunk_size : int
    trace_rows : array of bool
        array indicating which rows in df are to be traced

    Returns
    -------

    """

    if group_by_column_names is None:
        group_by_column_names = []

    rows_per_chunk, effective_chunk_size = \
        calc_rows_per_chunk(chunk_size, df, assignment_expressions,
                            extra_columns=len(group_by_column_names),
                            trace_label='eval_and_sum')

    logger.info("eval_and_sum chunk_size %s rows_per_chunk %s df rows %s" %
                (effective_chunk_size, rows_per_chunk, df.shape[0]))

    summary = None
    result_list = []
    trace_results = []
    trace_assigned_locals = {}

    for i, num_chunks, df_chunk, trace_rows_chunk in chunked_df(df, rows_per_chunk, trace_rows):

        logger.info("eval_and_sum chunk %s of %s" % (i, num_chunks))

        logger.debug("eval_and_sum chunk %s assign variables" % (i, ))
        assigned_chunk, trace_chunk, trace_assigned_locals_chunk = \
            assign.assign_variables(assignment_expressions,
                                    df_chunk,
                                    locals_dict=locals_dict,
                                    df_alias=df_alias,
                                    trace_rows=trace_rows_chunk)

        # sum this chunk
        logger.debug("eval_and_sum chunk %s sum" % (i, ))
        if group_by_column_names:
            # concat in the group_by columns
            for c in group_by_column_names:
                assigned_chunk[c] = df_chunk[c]
            # sum this chunk
            summary = assigned_chunk.groupby(group_by_column_names).sum()
        else:
            summary = assigned_chunk.sum().to_frame().T

        result_list.append(summary)

        if trace_chunk is not None:
            trace_results.append(trace_chunk)

        if trace_assigned_locals_chunk is not None:
            trace_assigned_locals.update(trace_assigned_locals_chunk)

        # note: chunk size will log low if there are more spec temp vars than extra_columns
        trace_label = 'eval_and_sum chunk_%s' % i
        chunk.log_open(trace_label, chunk_size, effective_chunk_size)
        chunk.log_df(trace_label, 'df_chunk', df_chunk)
        chunk.log_df(trace_label, 'assigned_chunk', assigned_chunk)
        chunk.log_close(trace_label)

    assert result_list

    # squash multiple chunk summaries
    if len(result_list) > 1:
        logger.debug("eval_and_sum squash chunk summaries")

        summary = pd.concat(result_list)

        if group_by_column_names:
            summary.reset_index(inplace=True)
            summary = summary.groupby(group_by_column_names).sum()
        else:
            summary = summary.sum().to_frame().T

    if trace_results:
        trace_results = pd.concat(trace_results)
        # trace_rows index values should match index of original df
        trace_results.index = df[trace_rows].index
    else:
        trace_results = None

    return summary, trace_results, trace_assigned_locals


def scalar_assign_variables(assignment_expressions, locals_dict):
    """
    Evaluate a set of variable expressions from a spec in the context
    of a given data table.

    Python expressions are evaluated in the context of this function using
    Python's eval function.
    Users should take care that these expressions must result in
    a scalar

    Parameters
    ----------
    assignment_expressions : pandas sequence of str
    locals_dict : Dict
        This is a dictionary of local variables that will be the environment
        for an evaluation of an expression that begins with @

    Returns
    -------
    variables : pandas.DataFrame
        Will have the index of `df` and columns of `exprs`.

    """

    # avoid trashing parameter when we add target
    locals_dict = locals_dict.copy() if locals_dict is not None else {}

    target_history = []
    # need to be able to identify which variables causes an error, which keeps
    # this from being expressed more parsimoniously
    for e in zip(assignment_expressions.target, assignment_expressions.expression):
        target = e[0]
        expression = e[1]

        # print "\n%s = %s" % (target, expression)

        try:
            if expression.startswith('@'):
                expression = expression[1:]

            value = eval(expression, globals(), locals_dict)

            # print "\n%s = %s" % (target, value)

            target_history.append((target, [value]))

            # FIXME - do we want to update locals to allows us to ref previously assigned targets?
            locals_dict[target] = value
        except Exception as err:
            logger.error("assign_variables failed target: %s expression: %s"
                         % (str(target), str(expression)))
            raise err

    # since we allow targets to be recycled, we want to only keep the last usage
    keepers = OrderedDict()
    for statement in reversed(target_history):
        # don't keep targets that staert with underscore
        # add statement to keepers list unless target is already in list
        target_name = statement[0]
        if not target_name.startswith('_') and target_name not in keepers:
            keepers[target_name] = statement[1]

    return pd.DataFrame.from_dict(keepers)
