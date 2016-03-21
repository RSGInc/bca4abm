import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def persons(data_dir, input_source, settings):

    df = bca.read_csv_or_stored_table(table_name="persons",
                                      data_dir=data_dir,
                                      input_source=input_source,
                                      settings=settings)

    return df


# merge table casting households onto persons
@orca.table()
def persons_merged(persons, households):
    return orca.merge_tables(target=persons.name,
                             tables=[persons, households])
