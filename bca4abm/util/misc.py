import orca
import pandas as pd


def drop_duplicates(seq):
    seen = set()
    seen_add = seen.add
    return [x for x in seq if not (x in seen or seen_add(x))]


def mapped_columns(*column_maps):
    '''
    Given any number of column_maps
    return a list of unique column names
    '''
    result = set()
    for column_map in column_maps:
        result |= set(column_map.values())

    return list(result)


def get_setting(key):
    settings = orca.eval_variable('settings')
    return settings.get(key)


def add_assigned_columns(base_dfname, from_df):

    for col in from_df.columns:
        # print "Adding %s to %s" % (col, base_dfname)
        orca.add_column(base_dfname, col, from_df[col])


def add_result_columns(base_dfname, from_df, prefix=''):

    for col_name in from_df.columns:
        dest_col_name = prefix + col_name
        # print "Adding result column %s to %s.%s" % (col_name, base_dfname, dest_col_name)
        orca.add_column(base_dfname, dest_col_name, from_df[col_name])


def add_targets_to_data_dictionary(targets, prefix, spec):

    if spec is None:
        return

    # map prefixed column name to description
    spec_dict = {prefix+e[0]: e[1] for e in zip(spec.target, spec.description)}

    data_dict = orca.get_injectable('data_dictionary')

    for col in targets:
        dest_col_name = prefix + col
        description = spec_dict.get(dest_col_name, '-')
        data_dict[dest_col_name] = description


def add_summary_results(df, summary_column_names=None, prefix='', spec=None):

    #  summarize all columns unless summary_column_names specifies a subset
    if summary_column_names is not None:
        df = df[summary_column_names]

    # if it has more than one row, sum the columns
    if df.shape[0] > 1:
        df = pd.DataFrame(df.sum()).T

    add_targets_to_data_dictionary(df.columns, prefix, spec)

    add_result_columns("summary_results", df, prefix)


def add_grouped_results(df, summary_column_names, prefix='', spec=None):
    # summarize everything
    coc_columns = orca.get_injectable('coc_column_names')

    if coc_columns == [None]:
        raise RuntimeError("add_grouped_results: coc_column_names not initialized"
                           " - did you forget to run demographics_processor?")

    grouped = df.groupby(coc_columns)

    aggregations = {column: 'sum' for column in summary_column_names}
    grouped = grouped.agg(aggregations)

    add_result_columns("coc_results", grouped, prefix)

    add_summary_results(grouped, prefix=prefix, spec=spec)


def missing_columns(table, expected_column_names):

    table_column_names = tuple(table.columns.values) + tuple(table.index.names)

    return list(c for c in expected_column_names if c not in table_column_names)


def extra_columns(table, expected_column_names):

    return list(c for c in table.columns.values if c not in expected_column_names)


def expect_columns(table, expected_column_names):

    missing_column_names = missing_columns(table, expected_column_names)
    extra_column_names = extra_columns(table, expected_column_names)

    for c in missing_column_names:
        print "expect_columns MISSING expected column %s" % c

    for c in extra_column_names:
        print "expect_columns FOUND unexpected column %s" % c

    if missing_column_names or extra_column_names:
        missing = ", ".join(missing_column_names)
        extra = ", ".join(extra_column_names)
        raise RuntimeError('expect_columns MISSING [%s] EXTRA:[%s]' % (missing, extra))

    return True
