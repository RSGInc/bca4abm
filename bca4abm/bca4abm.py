import numpy as np
import pandas as pd
import orca
import os
import yaml

from activitysim import activitysim as asim


def read_bca_table(table_name, index_col, data_dir, settings):

    # settings:
    #   <table_name>: <csv fiel name>
    #   <table_name>_column_map: { 'csv_col_name' : table_col_name', ... }
    #
    if table_name not in settings:
        return None

    fpath = os.path.join(data_dir, 'data', settings[table_name])

    column_map_key = table_name + "_column_map"
    if column_map_key in settings:
        usecols = settings[column_map_key].keys()
        # print "read_bca_table usecols: ", usecols
        df = pd.read_csv(fpath, header=0, usecols=usecols)
        df.rename(columns=settings[column_map_key], inplace=True)
    else:
        df = pd.read_csv(fpath, header=0)

    if index_col in df.columns:
        df.set_index(index_col, inplace=True)
    else:
        df.index.names = [index_col]

    return df


def expect_columns(table, expected_column_names):

    table_column_names = table.columns.values

    missing_column_names = list(c for c in expected_column_names if c not in table_column_names)
    extra_column_names = list(c for c in table_column_names if c not in expected_column_names)

    for c in missing_column_names:
        print "expect_columns MISSING expected column %s" % c

    for c in extra_column_names:
        print "expect_columns FOUND unexpected column %s" % c

    return missing_column_names == []


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

    print "read_assignment_spec", fname

    cfg = pd.read_csv(fname, comment='#')

    # drop null expressions
    # cfg = cfg.dropna(subset=[expression_name])

    # don't need description
    cfg = cfg.drop(description_name, axis=1)

    cfg.rename(columns={target_name: 'target',
                        expression_name: 'expression',
                        description_name: 'description'},
               inplace=True)

    return cfg


def assign_variables(assignment_expressions, df, locals_d=None):
    """
    Evaluate a set of variable expressions from a spec in the context
    of a given data table.

    There are two kinds of supported expressions: "simple" expressions are
    evaluated in the context of the DataFrame using DataFrame.eval.
    This is the default type of expression.

    Python expressions are evaluated in the context of this function using
    Python's eval function. Because we use Python's eval this type of
    expression supports more complex operations than a simple expression.
    Python expressions are denoted by beginning with the @ character.
    Users should take care that these expressions must result in
    a Pandas Series.

    Parameters
    ----------
    exprs : sequence of str
    df : pandas.DataFrame
    locals_d : Dict
        This is a dictionary of local variables that will be the environment
        for an evaluation of an expression that begins with @

    Returns
    -------
    variables : pandas.DataFrame
        Will have the index of `df` and columns of `exprs`.

    """
    if locals_d is None:
        locals_d = {}
    locals_d.update(locals())

    def to_series(x):
        if np.isscalar(x):
            return pd.Series([x] * len(df), index=df.index)
        return x

    l = []
    # need to be able to identify which variables causes an error, which keeps
    # this from being expressed more parsimoniously
    for e in zip(assignment_expressions.target, assignment_expressions.expression):
        target = e[0]
        expression = e[1]
        try:
            l.append((target, to_series(eval(expression[1:], globals(), locals_d))
                     if expression.startswith('@') else df.eval(expression)))
        except Exception as err:
            print "Variable %s expression failed for: %s" % (str(target), str(expression))
            raise err

    return pd.DataFrame.from_items(l)
