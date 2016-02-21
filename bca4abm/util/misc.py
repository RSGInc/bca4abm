import orca


def get_setting(key):
    settings = orca.eval_variable('settings')
    return settings.get(key)


def add_assigned_columns(base_dfname, from_df):
    for col in from_df.columns:
        print "Adding %s to %s" % (col, base_dfname)
        orca.add_column(base_dfname, col, from_df[col])


# use of this (hidden) utility function is a common idiom in activitysim.defaults
# from activitysim.defaults.models.utils.misc
def add_dependent_columns(base_dfname, new_dfname):
    tbl = orca.get_table(new_dfname)
    for col in tbl.columns:
        # print "Adding dependent", col
        orca.add_column(base_dfname, col, tbl[col])


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
