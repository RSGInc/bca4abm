# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.

import logging

from activitysim.core import inject


logger = logging.getLogger(__name__)


@inject.injectable(cache=True, override=True)
def chunk_size(settings):
    return int(settings.get('chunk_size', 0))


@inject.injectable(cache=True)
def hh_chunk_size(settings):
    if 'hh_chunk_size' in settings:
        return settings.get('hh_chunk_size', 0)
    else:
        return settings.get('chunk_size', 0)


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
