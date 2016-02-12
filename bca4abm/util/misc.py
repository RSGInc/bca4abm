import orca


def add_assigned_columns(base_dfname, from_df):
    for col in from_df.columns:
        print "Adding %s to %s" % (col, base_dfname)
        orca.add_column(base_dfname, col, from_df[col])


def missing_columns(table, expected_column_names):

    return list(c for c in expected_column_names if c not in table.columns.values)


def extra_columns(table, expected_column_names):

    return list(c for c in table.columns.values if c not in expected_column_names)


def expect_columns(table, expected_column_names):

    table_column_names = table.columns.values

    missing_column_names = list(c for c in expected_column_names if c not in table_column_names)
    extra_column_names = list(c for c in table_column_names if c not in expected_column_names)

    for c in missing_column_names:
        print "expect_columns MISSING expected column %s" % c

    for c in extra_column_names:
        print "expect_columns FOUND unexpected column %s" % c

    if missing_column_names or extra_column_names:
        missing = ", ".join(missing_column_names)
        extra = ", ".join(extra_column_names)
        raise RuntimeError('expect_columns MISSING [%s] EXTRA:[%s]' % (missing, extra))

    return True
