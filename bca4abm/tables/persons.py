import os.path
import numpy as np
import orca
import pandas as pd

from ..util.misc import read_bca_table


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def bca_persons_internal(data_dir, store, settings):

    if "bca_persons" in settings:
        df = read_bca_table("bca_persons", 'person_id', data_dir, settings)
    else:
        df = store["bca_persons"]

    return df


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def bca_persons(bca_persons_internal):
    return bca_persons_internal.to_frame()


@orca.table()
def bca_persons_merged(bca_persons, bca_households):
    return orca.merge_tables(target=bca_persons.name,
                             tables=[bca_persons, bca_households])
