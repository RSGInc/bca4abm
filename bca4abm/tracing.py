# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import os
import sys
import logging
import logging.config
import traceback


import yaml

import numpy as np
import pandas as pd
import orca


# Configurations
TRACE_LOGGER = 'bca4abm.trace'
BCA_LOGGER = 'bca4abm'
CSV_FILE_TYPE = 'csv'
TEXT_FILE_TYPE = 'txt'
LOGGING_CONF_FILE_NAME = 'logging.yaml'

# Tracers
tracers = {}


def print_stack_trace():

    traceback.print_stack()


def delete_csv_files(output_dir):
    """
    Delete CSV files

    Parameters
    ----------
    output_dir: str
        Directory of trace output CSVs

    Returns
    -------
    Nothing
    """
    for the_file in os.listdir(output_dir):
        if the_file.endswith(CSV_FILE_TYPE) or the_file.endswith(TEXT_FILE_TYPE):
            file_path = os.path.join(output_dir, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)


def log_file_path(name):
    """
    For use in logging.yaml tag to inject log file path

    filename: !!python/object/apply:bca4abm.defaults.tracing.log_file_path ['asim.log']

    Parameters
    ----------
    name: str
        output folder name

    Returns
    -------
    f: str
        output folder name
    """
    output_dir = orca.get_injectable('output_dir')
    f = os.path.join(output_dir, name)
    return f


def get_tracer(name=TRACE_LOGGER):
    """
    Get tracer

    Parameters
    ----------
    name: str
        tracer name

    Returns
    -------
    tracer: Tracer
        tracer
    """
    tracer = logging.getLogger(name)

    if (len(tracer.handlers) == 0):

        tracer.propagate = False
        tracer.setLevel(logging.INFO)

        file_path = log_file_path('%s.log' % name)
        fileHandler = logging.FileHandler(filename=file_path, mode='w')
        tracer.addHandler(fileHandler)

        logger = logging.getLogger(BCA_LOGGER)
        logger.info("Initialized tracer %s fileHandler %s" % (name, file_path))

    return tracer


def info(name=__name__, message=None, log=True):
    """
    write message to logger and/or tracer if household tracing enabled

    Parameters
    ----------
    logger: logger
        standard logger to write to (or not if None)
    message:
        logging message to write to logger and/or tracer

    Returns
    -------
    Nothing
    """
    if log:
        logging.getLogger(name).info(message)

    if orca.get_injectable('enable_trace_log'):
        get_tracer().info("%s - %s" % (name, message))


def debug(name=__name__, message=None, log=True):
    """
    same as info but debug
    """
    if log:
        logging.getLogger(name).debug(message)

    if orca.get_injectable('enable_trace_log'):
        get_tracer().debug("%s - %s" % (name, message))


def warn(name=__name__, message=None, log=True):
    """
    same as info but warn
    """
    if log:
        logging.getLogger(name).warn(message)

    if orca.get_injectable('enable_trace_log'):
        get_tracer().warn("%s - %s" % (name, message))


def error(name=__name__, message=None, log=True):
    """
    same as info but warn
    """
    if log:
        logging.getLogger(name).error(message)

    if orca.get_injectable('enable_trace_log'):
        get_tracer().error("%s - %s" % (name, message))


def config_logger(custom_config_file=None, basic=False):
    """
    Configure logger

    if log_config_file is not supplied then look for conf file in configs_dir

    if not found use basicConfig

    Parameters
    ----------
    custom_config_file: str
        custom config filename
    basic: boolean
        basic setup

    Returns
    -------
    Nothing
    """
    log_config_file = None

    if custom_config_file and os.path.isfile(custom_config_file):
        log_config_file = custom_config_file
    elif not basic:
        # look for conf file in configs_dir
        configs_dir = orca.get_injectable('configs_dir')
        default_config_file = os.path.join(configs_dir, LOGGING_CONF_FILE_NAME)
        if os.path.isfile(default_config_file):
            log_config_file = default_config_file

    if log_config_file:
        with open(log_config_file) as f:
            config_dict = yaml.load(f)
            config_dict = config_dict['logging']
            config_dict.setdefault('version', 1)
            logging.config.dictConfig(config_dict)
    else:
        logging.basicConfig(level=logging.INFO, stream=sys.stdout)

    logger = logging.getLogger(BCA_LOGGER)

    if custom_config_file and not os.path.isfile(custom_config_file):
        logger.error("#\n#\n#\nconfig_logger could not find conf file '%s'" % custom_config_file)

    if log_config_file:
        logger.info("Read logging configuration from: %s" % log_config_file)
    else:
        print "Configured logging using basicConfig"
        logger.info("Configured logging using basicConfig")

    output_dir = orca.get_injectable('output_dir')
    logger.info("Deleting files in output_dir %s" % output_dir)
    delete_csv_files(output_dir)


def write_locals(locals_dict, file_name):

    file_name = '%s.%s' % (file_name, TEXT_FILE_TYPE)

    file_path = log_file_path(file_name)

    mode = 'a' if os.path.isfile(file_path) else 'w'

    with open(file_path, mode=mode) as f:

        for key, value in locals_dict.iteritems():
            value = str(value)
            f.write("%s = %s\n" % (key, value))


def write_df_csv(df, file_path, index_label=None, columns=None, column_labels=None, transpose=True):

    mode = 'a' if os.path.isfile(file_path) else 'w'

    if columns:
        df = df[columns]

    if not transpose:
        df.to_csv(file_path, mmode="a", index=True, header=True)
        return

    df_t = df.transpose()
    if df.index.name is not None:
        df_t.index.name = df.index.name
    elif index_label:
        df_t.index.name = index_label

    with open(file_path, mode=mode) as f:
        if column_labels is None:
            column_labels = [None, None]
        if column_labels[0] is None:
            column_labels[0] = 'label'
        if column_labels[1] is None:
            column_labels[1] = 'value'

        if len(df_t.columns) == len(column_labels) - 1:
            column_label_row = ','.join(column_labels)
        else:
            column_label_row = \
                column_labels[0] + ',' \
                + ','.join([column_labels[1] + '_' + str(i+1) for i in range(len(df_t.columns))])

        if mode == 'a':
            column_label_row = '# ' + column_label_row
        f.write(column_label_row + '\n')
    df_t.to_csv(file_path, mode='a', index=True, header=True)


def write_series_csv(series, file_path, index_label=None, columns=None, column_labels=None):

    if isinstance(columns, str):
        series = series.rename(columns)
    elif isinstance(columns, list):
        if columns[0]:
            series.index.name = columns[0]
        series = series.rename(columns[1])
    if index_label and series.index.name is None:
        series.index.name = index_label
    series.to_csv(file_path, mode='a', index=True, header=True)


def write_csv(df, file_name, index_label=None, columns=None, column_labels=None, transpose=True):
    """
    Print write_csv

    Parameters
    ----------
    df: pandas.DataFrame or pandas.Series
        traced dataframe
    file_name: str
        output file name
    index_label: str
        index name
    columns: list
        columns to write
    transpose: bool
        whether to transpose dataframe (ignored for series)
    Returns
    -------
    Nothing
    """
    tracer = get_tracer()

    file_name = '%s.%s' % (file_name, CSV_FILE_TYPE)

    if isinstance(df, pd.Series):
        info(message="dumping %s element series to %s" % (len(df.index), file_name))
    else:
        info(message="dumping %s dataframe to %s" % (df.shape, file_name))

    file_path = log_file_path(file_name)

    if os.path.isfile(file_path):
        error(message="write_csv file exists %s %s" % (type(df).__name__, file_name))

    if isinstance(df, pd.DataFrame):
        write_df_csv(df, file_path, index_label, columns, column_labels, transpose=transpose)
    elif isinstance(df, pd.Series):
        write_series_csv(df, file_path, index_label, columns, column_labels)
    else:
        tracer.error("write_df_csv object '%s' of unexpected type: %s" % (file_name, type(df)))
