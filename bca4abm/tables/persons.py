import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_persons(data_dir, settings):
    return bca.read_csv_table(data_dir, settings,
                              table_name="persons",
                              column_map="persons_column_map")


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def persons(raw_persons):
    return raw_persons.to_frame()


# merge table casting households onto persons
@orca.table()
def persons_merged(persons, households):
    return orca.merge_tables(target=persons.name,
                             tables=[persons, households])
