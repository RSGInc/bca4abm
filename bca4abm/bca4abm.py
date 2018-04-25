# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import logging

import numpy as np
import pandas as pd
import os


from activitysim.core import config
from activitysim.core import inject
from activitysim.core import tracing
from activitysim.core import assign


logger = logging.getLogger(__name__)


def read_csv_or_tsv(fpath, header='infer', usecols=None, comment=None):

    if fpath.endswith('.tsv'):
        sep = '\t'
    elif fpath.endswith('.txt'):
        sep = '\s+'
    else:
        sep = ','

    return pd.read_csv(fpath, sep=sep, header=header, usecols=usecols, comment=comment)


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
        usecols = settings[column_map].keys()
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


def read_csv_or_stored_table(data_dir, input_source, settings, table_name, index_col=None):

    if input_source in ['read_from_csv', 'update_store_from_csv']:
        df = read_csv_table(data_dir, settings, table_name=table_name, index_col=index_col)
        if input_source == 'update_store_from_csv':
            print "updating store with table %s" % table_name
            with orca.eval_variable('input_store_for_update') as input_store:
                input_store[table_name] = df
    else:
        with orca.eval_variable('input_store_for_read') as input_store:
            print "reading table %s from store" % table_name
            df = input_store[table_name]

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

    # print "read_assignment_spec", fname

    cfg = pd.read_csv(fname, comment='#')

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
def size_chunked_df(df, trace_rows, chunk_size):
    # generator to iterate over chooses in chunk_size chunks
    if chunk_size == 0:
        yield 0, df, trace_rows
    else:
        num_choosers = len(df.index)
        i = 0
        while i < num_choosers:
            if trace_rows is None:
                yield i, df[i: i+chunk_size], None
            else:
                yield i, df[i: i+chunk_size], trace_rows[i: i+chunk_size]
            i += chunk_size


# generator for chunked iteration over dataframe by chunk_id
def id_chunked_df(df, trace_rows, chunk_id_col):
    # generator to iterate over chooses in chunk_size chunks
    last_chooser = df[chunk_id_col].max()
    i = 0
    while i <= last_chooser:
        chunk_me = (df[chunk_id_col] == i)
        if trace_rows is None:
            yield i, df[chunk_me], None
        else:
            yield i, df[chunk_me], trace_rows[chunk_me]
        i += 1


# return the appropriate generator for iterating over dataframe by either chunk_size or chunk_id
def chunked_df(df, trace_rows, chunk_size=None, chunk_id_col='chunk_id'):
    if chunk_size is None:
        return id_chunked_df(df, trace_rows, chunk_id_col)
    else:
        return size_chunked_df(df, trace_rows, chunk_size)


def eval_group_and_sum(assignment_expressions, df, locals_dict, group_by_column_names,
                       df_alias=None, chunk_size=0, trace_rows=None):

    if group_by_column_names == [None]:
        raise RuntimeError("eval_group_and_sum: group_by_column_names not initialized")

    summary = trace_results = trace_assigned_locals = None
    chunks = 0

    for i, df_chunk, trace_rows_chunk in chunked_df(df, trace_rows, chunk_size):

        chunks += 1

        # print "eval_and_sum chunk %s i: %s" % (chunks, i)

        assigned_chunk, trace_chunk, trace_assigned_locals_chunk = \
            assign.assign_variables(assignment_expressions,
                                    df_chunk,
                                    locals_dict=locals_dict,
                                    df_alias=df_alias,
                                    trace_rows=trace_rows_chunk)

        # concat in the group_by columns
        for c in group_by_column_names:
            assigned_chunk[c] = df_chunk[c]

        # sum this chunk
        chunk_summary = assigned_chunk.groupby(group_by_column_names).sum()

        # accumulate chunk_summaries in df
        if summary is None:
            summary = chunk_summary
        else:
            summary = pd.concat([summary, chunk_summary], axis=0)

        if trace_results is None:
            trace_results = trace_chunk
        elif trace_chunk is not None:
            trace_results = pd.concat([trace_results, trace_chunk], axis=0)

        if trace_assigned_locals is None:
            trace_assigned_locals = trace_assigned_locals_chunk
        elif trace_assigned_locals_chunk is not None:
            trace_assigned_locals.update(trace_assigned_locals_chunk)

    if chunks > 1:
        # squash the accumulated chunk summaries by reapplying group and sum
        summary.reset_index(inplace=True)
        summary = summary.groupby(group_by_column_names).sum()

        if trace_results is not None:
            # trace_rows index values should match index of original df
            trace_results.index = df[trace_rows].index

    return summary, trace_results, trace_assigned_locals


def eval_and_sum(assignment_expressions, df, locals_dict, df_alias=None,
                 chunk_size=0, trace_rows=None):

    summary = trace_results = trace_assigned_locals = trace_rows_chunk = None
    chunks = 0

    for i, df_chunk, trace_rows_chunk in chunked_df(df, trace_rows, chunk_size):

        chunks += 1

        # print "eval_and_sum chunk %s i: %s" % (chunks, i)

        assigned_chunk, trace_chunk, trace_assigned_locals_chunk = \
            assign.assign_variables(assignment_expressions,
                                    df_chunk,
                                    locals_dict=locals_dict,
                                    df_alias=df_alias,
                                    trace_rows=trace_rows_chunk)

        # sum this chunk
        chunk_summary = assigned_chunk.sum()

        # accumulate chunk_summaries in df
        if summary is None:
            summary = chunk_summary
        else:
            summary = pd.concat([summary, chunk_summary], axis=0)

        if trace_results is None:
            trace_results = trace_chunk
        elif trace_chunk is not None:
            trace_results = pd.concat([trace_results, trace_chunk], axis=0)

        if trace_assigned_locals is None:
            trace_assigned_locals = trace_assigned_locals_chunk
        elif trace_assigned_locals_chunk is not None:
            trace_assigned_locals.update(trace_assigned_locals_chunk)

    if chunks > 1:
        # squash the accumulated chunk summaries by reapplying group and sum
        summary = summary.sum()

        # trace_rows index values should match index of original df
        trace_results.index = df[trace_rows].index

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
    exprs : sequence of str
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

            value = eval(expression, globals(), locals_dict)

            # print "\n%s = %s" % (target, value)

            l.append((target, [value]))

            # FIXME - do we want to update locals to allows us to ref previously assigned targets?
            locals_dict[target] = value
        except Exception as err:
            logger.error("assign_variables failed target: %s expression: %s"
                         % (str(target), str(expression)))
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
