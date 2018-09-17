# bca4abm
# See full license in LICENSE.txt.

import logging

import os
import re
import pandas as pd
import numpy as np
import openmatrix as omx


from bca4abm import bca4abm as bca
from ...util.misc import add_aggregate_results

from activitysim.core import config
from activitysim.core import inject
from activitysim.core import tracing
from activitysim.core import pipeline
from activitysim.core import assign

logger = logging.getLogger(__name__)

"""
Aggregate OD processor

each row in the data table to solve is an OD pair and this processor
calculates trip differences.  It requires the access to input zone tables,
the COC coding, trip matrices and skim matrices.  The new
OD_aggregate_manifest.csv file tells this processor what data it can
use and how to reference it.  The following input data tables are required:
assign_mfs.omx, inputs and results of the zone aggregate processor, and skims_mfs.omx.
"""


class ODSkims(object):
    """
    Wrapper for skim arrays to facilitate use of skims by aggregate_od_processor

    Parameters
    ----------
    skims_dict : empty dict to cache skims read from file
    omx: open omx file object
        this is only used to load skims on demand that were not preloaded
    length: int
        number of zones in skim to return in skim matrix
        in case the skims contain additional external zones that should be trimmed out so skim
        array is correct shape to match (flattened) O-D tiled columns in the od dataframe
    transpose: bool
        whether to transpose the matrix before flattening. (i.e. act as a D-O instead of O-D skim)
    """

    def __init__(self, omx_file_path, name, length, transpose=False, cache_skims=True):

        self.skims = {}

        self.name = name
        self.transpose = transpose
        self.length = length

        self.omx = omx.open_file(omx_file_path, 'r')
        self.omx_shape = tuple([int(s) for s in self.omx.shape()])
        self.skim_dtype = np.float64

        self.cache_skims = cache_skims

        self.usage = {key: 0 for key in self.omx.listMatrices()}

        logger.debug("omx file %s skim shape: %s number of skims: %s" %
                     (name, self.omx_shape, len(self.usage)))

    def __getitem__(self, key):
        """
        accessor to return flattened skim array with specified key
        flattened array will have length length*length and will match tiled OD df used by asim_eval

        this allows the skim array to be accessed from expressions as
        skim['DISTANCE'] or skim[('SOVTOLL_TIME', 'MD')]
        """

        assert key in self.usage

        self.usage[key] += 1

        if key in self.skims:
            # logger.debug("ODSkims using cached %s from omx %s" % (key, self.name, ))
            data = self.skims[key]
        else:
            # logger.debug("ODSkims loading %s from omx %s" % (key, self.name, ,))
            data = self.get_from_omx(key)
            if self.cache_skims:
                self.skims[key] = data

        return data

    def get_from_omx(self, key):

        if isinstance(key, str):
            omx_key = key
        elif isinstance(key, tuple):
            omx_key = '__'.join(key)
        else:
            raise RuntimeError("Unexpected skim key type %s" % type(key))

        try:
            data = self.omx[omx_key][:self.length, :self.length]
        except omx.tables.exceptions.NoSuchNodeError:
            raise RuntimeError("Could not find skim with key '%s' in %s" % (omx_key, self.name))

        if self.transpose:
            data = data.transpose()

        return data.flatten()

    def log_skim_usage(self):

        num_skims = len(self.usage)
        num_used = (np.asanyarray(self.usage.values()) > 0).sum()
        num_unused = num_skims - num_used

        avg_used = (np.asanyarray(self.usage.values())).sum() / float(num_used or 1)
        max_used = np.asanyarray(self.usage.values()).max()

        logger.debug("  %s (cached=%s) %s skims in omx file %s used (%s avg %s max) %s unused" %
                     (self.name, self.cache_skims,
                      num_skims, num_used, avg_used, max_used, num_unused))

        # for key, n in self.usage.iteritems():
        #     if n > 4:
        #         logger.info("%s.%s %s" % (self.name, key, n))

    def close(self):

        self.omx.close()
        self.skims = {}


def add_skims_to_locals(full_local_name, omx_file_name, zone_count, local_od_skims, cache_skims):

        logger.debug("add_skims_to_locals: %s : %s" % (full_local_name, omx_file_name))

        omx_file = omx.open_file(omx_file_name, 'r')

        # for skimName in omx_file.listMatrices():
        #     print "aggregate_od_matrices %s: '%s'" % (full_local_name, skimName)

        skims = ODSkims(name=full_local_name,
                        length=zone_count,
                        omx=omx_file,
                        cache_skims=cache_skims)

        local_od_skims[full_local_name] = skims


def create_skim_locals_dict(model_settings, data_dir, zone_count, cache_skims):

    aggregate_od_matrices = model_settings.get('aggregate_od_matrices', None)
    if not aggregate_od_matrices:
        raise RuntimeError("No list of aggregate_od_matrices found in model_settings")

    local_od_skims = {}
    for local_name, omx_file_name in aggregate_od_matrices.iteritems():

        for scenario in ['base', 'build']:
            full_local_name = '_'.join([local_name, scenario])
            data_sub_dir = '%s-data' % scenario
            omx_file_path = os.path.join(data_dir, data_sub_dir, omx_file_name)

            logger.debug("add od_skims to locals: %s : %s" % (full_local_name, omx_file_name))

            skims = ODSkims(omx_file_path=omx_file_path,
                            name=full_local_name,
                            length=zone_count,
                            cache_skims=cache_skims)

            local_od_skims[full_local_name] = skims

    return local_od_skims


@inject.step()
def aggregate_od_processor(
        zone_districts,
        data_dir, trace_od):

    trace_label = 'aggregate_od'

    logger.info("Running %s" % (trace_label, ))

    model_settings = config.read_model_settings('aggregate_od.yaml')

    spec_file_name = model_settings.get('spec_file_name', 'aggregate_od.csv')
    aggregate_od_spec = bca.read_assignment_spec(spec_file_name)

    zones = zone_districts.to_frame()
    zone_count = zones.shape[0]

    # create OD dataframe in order compatible with ODSkims
    od_df = pd.DataFrame(
        data={
            'orig': np.repeat(np.asanyarray(zones.index), zone_count),
            'dest': np.tile(np.asanyarray(zones.index), zone_count),
        }
    )

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = config.get_model_constants(model_settings)
    locals_dict.update(config.setting('globals'))
    locals_dict['logger'] = logger

    # add ODSkims to locals (note: we use local_skims list later to close omx files)
    cache_skims = model_settings.get('cache_skims', False)
    local_skims = create_skim_locals_dict(model_settings, data_dir, zone_count, cache_skims)
    locals_dict.update(local_skims)

    if trace_od:
        trace_orig, trace_dest = trace_od
        trace_od_rows = (od_df.orig == trace_orig) & (od_df.dest == trace_dest)
    else:
        trace_od_rows = None

    logger.debug("%s assigning variables" % (trace_label,))
    results, trace_results, trace_assigned_locals = \
        assign.assign_variables(aggregate_od_spec,
                                od_df,
                                locals_dict=locals_dict,
                                df_alias='od',
                                trace_rows=trace_od_rows)

    for local_name, od_skims in local_skims.iteritems():
        logger.debug("closing %s" % local_name)
        od_skims.log_skim_usage()
        od_skims.close()

    # summarize aggregate_od_benefits by orig and dest districts
    logger.debug("%s district summary" % (trace_label,))
    results['orig'] = np.repeat(np.asanyarray(zones.district), zone_count)
    results['dest'] = np.tile(np.asanyarray(zones.district), zone_count)
    district_summary = results.groupby(['orig', 'dest']).sum()
    pipeline.extend_table('aggregate_od_district_summary', district_summary, axis=1)

    # attribute aggregate_results benefits to origin zone
    logger.debug("%s zone summary" % (trace_label,))
    results['orig'] = od_df['orig']
    del results['dest']
    zone_summary = results.groupby(['orig']).sum()
    pipeline.extend_table('aggregate_od_zone_summary', zone_summary, axis=1)

    add_aggregate_results(zone_summary, aggregate_od_spec, source=trace_label)

    if trace_results is not None:
        tracing.write_csv(trace_results,
                          file_name=trace_label,
                          index_label='index',
                          column_labels=['label', 'od'])

        if trace_assigned_locals:
            tracing.write_csv(trace_assigned_locals,
                              file_name="%s_locals" % trace_label,
                              index_label='variable', columns='value')
