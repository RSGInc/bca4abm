import orca


def add_assigned_columns(base_dfname, from_df):
    for col in from_df.columns:
        print "Adding %s to %s" % (col, base_dfname)
        orca.add_column(base_dfname, col, from_df[col])
