# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import os.path
import logging

import pandas as pd
import yaml

from activitysim.core import inject


logger = logging.getLogger(__name__)


#clean up needed

@inject.injectable(cache=True, override=True)
def chunk_size(settings):
    return int(settings.get('chunk_size', 0))


@inject.injectable(cache=True)
def hh_chunk_size(settings):
    if 'hh_chunk_size' in settings:
        return settings.get('hh_chunk_size', 0)
    else:
        return settings.get('chunk_size', 0)

#
# @inject.injectable()
# def output_store_file_name(settings):
#     return settings.get("output_store", 'bca_results.h5')
#
#
# @inject.injectable()
# def output_store(output_dir, output_store_file_name):
#
#     output_store_path = os.path.join(output_dir, output_store_file_name)
#
#     print "opening output_store for UPDATE", output_store_path
#
#     return pd.HDFStore(output_store_path)
#
#
# @inject.injectable()
# def output_store_for_read(output_dir, output_store_file_name):
#
#     output_store_path = os.path.join(output_dir, output_store_file_name)
#
#     print "opening output_store for READ", output_store_path
#
#     return pd.HDFStore(output_store_path, mode='r')


@inject.injectable()
def input_source(settings):
    source = 'read_from_csv'
    # source = 'read_from_store'
    # source = 'update_store_from_csv'

    return settings.get("input_source", source)


@inject.injectable()
def input_store_file_name(settings):
    return settings.get("input_store", 'store.h5')


@inject.injectable()
def input_store_for_update(data_dir, input_store_file_name):

    input_store_path = os.path.join(data_dir, input_store_file_name)

    # print "opening input_store for UPDATE", input_store_path

    return pd.HDFStore(input_store_path)


@inject.injectable()
def input_store_for_read(data_dir, input_store_file_name):

    input_store_path = os.path.join(data_dir, input_store_file_name)

    # print "opening input_store for READ", input_store_path

    return pd.HDFStore(input_store_path, mode='r')


@inject.injectable()
def input_store_for_write(data_dir, input_store_file_name):

    input_store_path = os.path.join(data_dir, input_store_file_name)

    # print "opening input_store for READ", input_store_path

    return pd.HDFStore(input_store_path, mode='w')


@inject.injectable(cache=True, override=True)
def trace_hh_id(settings):

    id = settings.get('trace_hh_id', None)

    if id and not isinstance(id, int):
        logger.warn("setting trace_hh_id is wrong type, should be an int, but was %s" % type(id))
        id = None

    return id


@inject.injectable(cache=True, override=True)
def trace_od(settings):

    od = settings.get('trace_od', None)

    if od and not (isinstance(od, list) and len(od) == 2 and all(isinstance(x, int) for x in od)):
        logger.warn("setting trace_od is wrong type, should be a list of length 2, but was %s" % od)
        od = None

    return od


