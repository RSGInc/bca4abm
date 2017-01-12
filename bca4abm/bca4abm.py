# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import logging

import numpy as np
import pandas as pd
import orca
import os


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


def undupe_column_names(df, template="{} ({})"):
    """
    rename df column names so there are no duplicates (in place)

    e.g. if there are two columns named "dog", the second column will be reformatted to "dog (2)"
    Parameters
    ----------
    df : pandas.DataFrame
        dataframe whose column names should be de-duplicated
    template : template taking two arguments (old_name, int) to use to rename columns

    Returns
    -------
    df : pandas.DataFrame
        dataframe that was renamed in place, for convenience in chaining
    """
    new_names = []
    seen = set()
    for name in df.columns:
        n = 1
        new_name = name
        while new_name in seen:
            n += 1
            new_name = template.format(name, n)
        new_names.append(new_name)
        seen.add(new_name)
    df.columns = new_names
    return df


class NumpyLogger(object):
    def __init__(self, logger):
        self.logger = logger
        self.target = ''
        self.expression = ''

    def write(self, msg):
        self.logger.error("numpy warning: %s" % (msg.rstrip()))
        self.logger.error("expression: %s = %s" % (str(self.target), str(self.expression)))


def assign_variables(assignment_expressions, df, locals_dict, df_alias=None, trace_rows=None):
    """
    Evaluate a set of variable expressions from a spec in the context
    of a given data table.

    Expressions are evaluated using Python's eval function.
    Python expressions have access to variables in locals_d (and df being
    accessible as variable df.) They also have access to previously assigned
    targets as the assigned target name.

    variables starting with underscore are considered temps variables and returned seperately
    (and only if return_temp_variables is true)

    Users should take care that expressions should result in
    a Pandas Series (scalars will be automatically promoted to series.)

    Parameters
    ----------
    assignment_expressions : pandas.DataFrame of target assignment expressions
        target: target column names
        expression: pandas or python expression to evaluate
    df : pandas.DataFrame
    locals_d : Dict
        This is a dictionary of local variables that will be the environment
        for an evaluation of "python" expression.
    trace_rows: series or array of bools to use as mask to select target rows to trace

    Returns
    -------
    variables : pandas.DataFrame
        Will have the index of `df` and columns named by target and containing
        the result of evaluating expression
    trace_df : pandas.DataFrame or None
        a dataframe containing the eval result values for each assignment expression
    """

    np_logger = NumpyLogger(logger)

    def is_local(target):
        return target.startswith('_') and target.isupper()

    def is_temp(target):
        return target.startswith('_')

    def to_series(x, target=None):
        if x is None or np.isscalar(x):
            if target:
                logger.warn("WARNING: assign_variables promoting scalar %s to series" % target)
            return pd.Series([x] * len(df.index), index=df.index)
        return x

    trace_assigned_locals = trace_results = None
    if trace_rows is not None:
        # convert to numpy array so we can slice ndarrays as well as series
        trace_rows = np.asanyarray(trace_rows)
        if trace_rows.any():
            trace_results = []
            trace_assigned_locals = {}

    # avoid touching caller's passed-in locals_d parameter (they may be looping)
    locals_dict = locals_dict.copy() if locals_dict is not None else {}
    if df_alias:
        locals_dict[df_alias] = df
    else:
        locals_dict['df'] = df
    local_keys = locals_dict.keys()

    l = []
    # need to be able to identify which variables causes an error, which keeps
    # this from being expressed more parsimoniously
    for e in zip(assignment_expressions.target, assignment_expressions.expression):
        target, expression = e

        if target in local_keys:
            logger.warn("assign_variables target obscures local_d name '%s'" % str(target))

        if is_local(target):
            x = eval(expression, globals(), locals_dict)
            locals_dict[target] = x
            if trace_assigned_locals is not None:
                trace_assigned_locals[target] = x
            continue

        try:

            def log_numpy_err(type, flag):
                logger.error("assign_variables warning: %s: %s" % (type(err).__name__, str(err)))

                logger.error("assign_variables expression: %s = %s"
                             % (str(target), str(expression)))

                print("numpy error (%s), with flag %s" % (type, flag))

            # saved_handler = np.seterrcall(log_numpy_err)
            # save_err = np.seterr(all='call')

            # FIXME - log numpy warnings
            np_logger.target = str(target)
            np_logger.expression = str(expression)
            saved_handler = np.seterrcall(np_logger)
            save_err = np.seterr(all='log')

            values = to_series(eval(expression, globals(), locals_dict), target=target)

            np.seterr(**save_err)
            np.seterrcall(saved_handler)

        except Exception as err:
            logger.error("assign_variables error: %s: %s" % (type(err).__name__, str(err)))

            logger.error("assign_variables expression: %s = %s"
                         % (str(target), str(expression)))

            # values = to_series(None, target=target)
            raise err

        l.append((target, values))

        if trace_results is not None:
            trace_results.append((target, values[trace_rows]))

        # update locals to allows us to ref previously assigned targets
        locals_dict[target] = values

    # build a dataframe of eval results for non-temp targets
    # since we allow targets to be recycled, we want to only keep the last usage
    # we scan through targets in reverse order and add them to the front of the list
    # the first time we see them so they end up in execution order
    variables = []
    seen = set()
    for statement in reversed(l):
        # statement is a tuple (<target_name>, <eval results in pandas.Series>)
        target_name = statement[0]
        if not is_temp(target_name) and target_name not in seen:
            variables.insert(0, statement)
            seen.add(target_name)

    # DataFrame from list of tuples [<target_name>, <eval results>), ...]
    variables = pd.DataFrame.from_items(variables)

    if trace_results is not None:

        trace_results = pd.DataFrame.from_items(trace_results)
        trace_results.index = df[trace_rows].index

        trace_results = undupe_column_names(trace_results)

        # add df columns to trace_results
        trace_results = pd.concat([df[trace_rows], trace_results], axis=1)

    return variables, trace_results, trace_assigned_locals


def assign_variables_locals(settings, settings_tag=None):
    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = {
        'log': np.log,
        'exp': np.exp
    }
    if 'globals' in settings:
        locals_dict.update(settings.get('globals'))
    if settings_tag:
        locals_tag = "locals_%s" % settings_tag
        if locals_tag in settings:
            locals_dict.update(settings.get(locals_tag))
    return locals_dict


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
            assign_variables(assignment_expressions,
                             df_chunk,
                             locals_dict=locals_dict,
                             df_alias=df_alias,
                             trace_rows=trace_rows_chunk)

        # concat in the group_by columns
        assigned_chunk = pd.concat([df_chunk[group_by_column_names], assigned_chunk], axis=1)

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
            assign_variables(assignment_expressions,
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
