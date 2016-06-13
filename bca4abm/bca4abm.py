# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import numpy as np
import pandas as pd
import orca
import os
import yaml

from .util.misc import expect_columns


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


def read_assignment_spec(fname,
                         description_name="Description",
                         target_name="Target",
                         expression_name="Expression"):
    """
    Read a CSV model specification into a Pandas DataFrame or Series.

    The CSV is expected to have columns for component descriptions
    targets, and expressions,

    The CSV is required to have a header with column names. For example:

        Description,Target,Expression

    Parameters
    ----------
    fname : str
        Name of a CSV spec file.
    description_name : str, optional
        Name of the column in `fname` that contains the component description.
    target_name : str, optional
        Name of the column in `fname` that contains the component target.
    expression_name : str, optional
        Name of the column in `fname` that contains the component expression.

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

    cfg.rename(columns={target_name: 'target',
                        expression_name: 'expression',
                        description_name: 'description'},
               inplace=True)

    # backfill description
    if 'description' not in cfg.columns:
        cfg.description = ''

    cfg.target = cfg.target.str.strip()
    cfg.expression = cfg.expression.str.strip()

    return cfg


def assign_variables(assignment_expressions, df, locals_d):
    """
    Evaluate a set of variable expressions from a spec in the context
    of a given data table.

    Expressions are evaluated using Python's eval function.
    Python expressions have access to variables in locals_d (and df being
    accessible as variable df.) They also have access to previously assigned
    targets as the assigned target name.

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

    Returns
    -------
    variables : pandas.DataFrame
        Will have the index of `df` and columns of `exprs`.

    """

    def to_series(x, target=None):
        if np.isscalar(x):
            if target:
                print "WARNING: assign_variables promoting scalar %s to series" % target
            return pd.Series([x] * len(df), index=df.index)
        return x

    # avoid trashing parameter when we add target
    locals_d = locals_d.copy() if locals_d is not None else {}
    locals_d['df'] = df

    l = []
    # need to be able to identify which variables causes an error, which keeps
    # this from being expressed more parsimoniously
    for e in zip(assignment_expressions.target, assignment_expressions.expression):
        target = e[0]
        expression = e[1]
        try:
            values = to_series(eval(expression, globals(), locals_d), target=target)
            l.append((target, values))

            # FIXME - do we want to update locals to allows us to ref previously assigned targets?
            locals_d[target] = values
        except Exception as err:
            print "Variable %s expression failed for: %s" % (str(target), str(expression))
            raise err

    # since we allow targets to be recycled, we want to only keep the last usage
    keepers = []
    for statement in reversed(l):
        # don't keep targets that start with underscore
        if statement[0].startswith('_'):
            continue
        # add statement to keepers list unless target is already in list
        if not next((True for keeper in keepers if keeper[0] == statement[0]), False):
            keepers.append(statement)

    return pd.DataFrame.from_items(keepers)


def assign_variables_locals(settings, settings_locals=None):
    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d = {
        'settings': settings,
        'log': np.log
    }
    locals_d.update(settings['locals'])
    if settings_locals and settings_locals in settings:
        locals_d.update(settings[settings_locals])
    return locals_d
