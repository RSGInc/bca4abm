import os
import orca
import pandas as pd
import numpy as np
import openmatrix as omx


from bca4abm import bca4abm as bca
from ...util.misc import add_summary_results
from ...util.misc import add_aggregate_results

from bca4abm import tracing


"""
Aggregate OD processor
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

    def __init__(self, name, length, omx, transpose=False, skims_dict={}):

        # allows skims dict to be shared between OD and transposed (DO) ODSkims objects
        self.skims = skims_dict

        self.length = length
        self.name = name
        self.transpose = transpose

        self.omx = omx

    def __getitem__(self, key):
        """
        accessor to return flattened skim array with specified key
        flattened array will have length length*length and will match tiled OD df used by asim_eval

        this allows the skim array to be accessed from expressions as
        skim['DISTANCE'] or skim[('SOVTOLL_TIME', 'MD')]
        """

        if key not in self.skims:
            self.get_from_omx(key)

        data = self.skims[key]

        if self.transpose:
            return data.transpose().flatten()
        else:
            return data.flatten()

    def get_from_omx(self, key):
        if isinstance(key, str):
            omx_key = key
        elif isinstance(key, tuple):
            omx_key = '__'.join(key)
        else:
            raise RuntimeError("Unexpected skim key type %s" % type(key))
        tracing.info(__name__,
                     message="ODSkims loading %s from omx %s as %s" % (key, self.name, omx_key,))

        print "ODSkims loading %s from omx %s as %s" % (key, self.name, omx_key,)
        try:
            self.skims[key] = self.omx[omx_key][:self.length, :self.length]
        except omx.tables.exceptions.NoSuchNodeError:
            raise RuntimeError("Could not find skim with key '%s' in %s" % (omx_key, self.name))


@orca.injectable()
def aggregate_od_spec(configs_dir):

    f = os.path.join(configs_dir, "aggregate_od.csv")
    return bca.read_assignment_spec(f)


def add_skims_to_locals(full_local_name, omx_file_name, zone_count, local_od_skims):

        print "add_skims_to_locals: %s : %s" % (full_local_name, omx_file_name)

        omx_file = omx.open_file(omx_file_name, 'r')

        # for skimName in omx_file.listMatrices():
        #     print "aggregate_od_matrices %s: '%s'" % (full_local_name, skimName)

        skims = ODSkims(name=full_local_name,
                        length=zone_count,
                        omx=omx_file)

        local_od_skims[full_local_name] = skims


def create_skim_locals_dict(settings, data_dir, zone_count):

    aggregate_od_matrices = settings.get('aggregate_od_matrices', None)
    if not aggregate_od_matrices:
        raise RuntimeError("No list of aggregate_od_matrices found in settings")

    local_od_skims = {}
    for local_name, omx_file_name in aggregate_od_matrices.iteritems():

        for scenario in ['base', 'build']:
            full_local_name = '_'.join([local_name, scenario])
            data_sub_dir = '%s-data' % scenario
            omx_file_path = os.path.join(data_dir, data_sub_dir, omx_file_name)
            add_skims_to_locals(full_local_name, omx_file_path, zone_count, local_od_skims)

    return local_od_skims


@orca.step()
def aggregate_od_processor(zone_demographics, aggregate_od_spec, settings, data_dir, trace_od):

    print "---------- aggregate_od_processor"

    tracing.info(__name__, "Running aggregate_od_processor")

    tracing.write_csv(zone_demographics.to_frame(), file_name="zone_demographics", transpose=False)

    zones_index = zone_demographics.index
    zone_count = len(zones_index)

    coc_end = settings.get('trip_coc_end', 'not found')
    if coc_end not in ['orig', 'dest']:
        raise RuntimeError("setting trip_coc_end (%s) should be orig or dest" % coc_end)

    # create OD dataframe
    od_df = pd.DataFrame(
        data={
            'orig': np.repeat(np.asanyarray(zones_index), zone_count),
            'dest': np.tile(np.asanyarray(zones_index), zone_count)
        }
    )

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_dict = bca.assign_variables_locals(settings, 'aggregate_od')

    # add ODSkims to locals (note: we use local_skims list later to close omx files)
    local_skims = create_skim_locals_dict(settings, data_dir, zone_count)
    locals_dict.update(local_skims)

    if trace_od:
        trace_orig, trace_dest = trace_od
        trace_od_rows = (od_df.orig == trace_orig) & (od_df.dest == trace_dest)
    else:
        trace_od_rows = None

    results, trace_results, trace_assigned_locals = \
        bca.eval_group_and_sum(assignment_expressions=aggregate_od_spec,
                               df=od_df,
                               locals_dict=locals_dict,
                               df_alias='od',
                               group_by_column_names=[coc_end],
                               chunk_size=0,
                               trace_rows=trace_od_rows)

    add_aggregate_results(results, aggregate_od_spec, source='aggregate_od')

    if settings.get("dump", False) and settings.get("dump_aggregate_od", True):
        output_dir = orca.eval_variable('output_dir')
        csv_file_name = os.path.join(output_dir, 'aggregate_od_benefits.csv')
        print "writing", csv_file_name
        results.to_csv(csv_file_name, index=False)

    for local_name, od_skims in local_skims.iteritems():
        print "closing %s" % od_skims.name
        od_skims.omx.close

    if trace_assigned_locals is not None:
        tracing.write_locals(trace_assigned_locals,
                             file_name="aggregate_od_locals")

    if trace_results is not None:
        tracing.write_csv(trace_results,
                          file_name="aggregate_od",
                          index_label='index',
                          column_labels=['label', 'od'])
