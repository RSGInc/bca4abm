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


def read_and_concat_csv_files(data_dir, file_names, axis=1):

    omnibus_df = None

    for file_name in file_names:

        df = read_csv_file(data_dir=data_dir, file_name=file_name)

        if omnibus_df is None:
            omnibus_df = df
        else:
            omnibus_df = pd.concat([omnibus_df, df], axis=axis)

    return omnibus_df


@orca.table(cache=True)
def zone_cvals(data_dir, settings):

    file_name = settings.get('cval_file_name')

    base_cvals_df = read_csv_file(
        data_dir=os.path.join(data_dir, 'base-data'),
        file_name=file_name,
        column_map=None)

    build_cvals_df = read_csv_file(
        data_dir=os.path.join(data_dir, 'build-data'),
        file_name=file_name,
        column_map=None)

    #add external cocs
    cocs_file_names = settings.get('ext_cocs_file_name')
    base_cocs_df = read_csv_file(data_dir=os.path.join(data_dir, 'base-data'), file_name=cocs_file_names)
    base_cvals_df = pd.concat([base_cvals_df, base_cocs_df])
    build_cocs_df = read_csv_file(data_dir=os.path.join(data_dir, 'build-data'), file_name=cocs_file_names)
    build_cvals_df = pd.concat([build_cvals_df, build_cocs_df])
    
    base_cvals_df.columns = ['base_%s' % c for c in base_cvals_df.columns.values]
    build_cvals_df.columns = ['build_%s' % c for c in build_cvals_df.columns.values]

    cvals_df = pd.concat([base_cvals_df, build_cvals_df], axis=1)

    cvals_df.index = cvals_df.index + 1
    cvals_df.index.name = 'ZONE'

    # print "cvals_df: ", cvals_df.columns.values

    return cvals_df


@orca.table(cache=True)
def zones(data_dir, settings):
    """
    aggregate_zone_file_names in settings contains a list of file name
    for zones table csv data input files (expect versions in build and base data subdirs)
    data will be combined into a single table with columns names prefixed with 'base_' or 'build_'
    (e.g.) if ma.hbcdcls.csv has a column 'hbcdcls' you will have 'base_hbcdcls' and 'build_hbcdcls'
    """

    file_names = settings.get('aggregate_zone_file_names')

    base_zones_df = read_and_concat_csv_files(
        data_dir=os.path.join(data_dir, 'base-data'),
        file_names=file_names,
        axis=1
    )

    build_zones_df = read_and_concat_csv_files(
        data_dir=os.path.join(data_dir, 'build-data'),
        file_names=file_names,
        axis=1
    )

    base_zones_df.columns = ['base_%s' % c for c in base_zones_df.columns.values]
    build_zones_df.columns = ['build_%s' % c for c in build_zones_df.columns.values]

    zones_df = pd.concat([base_zones_df, build_zones_df], axis=1)

    # the default index is zero-based, so we can convert to 1-based zone ids simply by adding 1
    zones_df.index = zones_df.index + 1
    zones_df.index.name = 'ZONE'

    # print "zones_df: ", zones_df.columns.values

    return zones_df
