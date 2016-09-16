import os.path
import numpy as np
import orca
import pandas as pd
import itertools

from bca4abm import bca4abm as bca


def conflate_cval(cval, in_place=True):

    if in_place:
        cval_out = cval
    else:
        cval_out = pd.DataFrame(index=cval.index)

    a = 1+np.arange(4)
    for i in itertools.product(a, a, a, a):
        c = "a%si%sh%sw%s" % i
        rhs_cols = ["%sc%s" % (c, j) for j in a]
        # print "%s = %s" % (c, rhs_cols)
        # row sum
        cval_out[c] = cval[rhs_cols].sum(axis=1)

    return cval_out


def read_csv_file(data_dir, file_name, column_map=None):

    fpath = os.path.join(data_dir, file_name)

    if column_map:
        usecols = column_map.keys()
        # print "read_bca_table usecols: ", usecols
        # FIXME - should we allow comment lines?
        df = bca.read_csv_or_tsv(fpath, header=0, usecols=usecols)
        df.rename(columns=column_map, inplace=True)
    else:
        df = bca.read_csv_or_tsv(fpath, header=0, comment='#')

    return df


@orca.table(cache=True)
def zone_demographics(data_dir, input_source, settings):

    column_map = None
    file_name = settings.get('zone_demographics')

    demographics_data_dir = os.path.join(data_dir, settings.get('zone_demographics_subdir', ''))

    zones_df = read_csv_file(
        data_dir=demographics_data_dir,
        file_name=file_name,
        column_map=column_map)

    zones_df.index = zones_df.index + 1
    zones_df.index.name = 'ZONE'

    zones_df = conflate_cval(zones_df)

    return zones_df
