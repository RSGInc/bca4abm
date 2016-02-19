import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from bca4abm.util.misc import expect_columns


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


# FIXME - sample of a dependent column that remaps categories
# convert person type categories to string descriptors
@orca.column("persons")
def person_cat(persons, settings):
    return persons.person_type.map(settings["person_type_map"])


# FIXME - sample of a dependent column that remaps categories
# convert person type categories to string descriptors
@orca.column("persons")
def person_gender_cat(persons, settings):
    return persons.person_gender.map(settings["gender_map"])


# FIXME - sample of a dependent column
@orca.column("persons")
def adult(persons):
    return persons.to_frame(["person_age"]).eval("18 <= person_age")


# FIXME - remove this if there are no demographics columns dependent
# this is the placeholder for all the columns to update after the demographic processor runs
@orca.table()
def persons_demographics(persons):
    return pd.DataFrame(index=persons.index)


# FIXME - replace this with any column dependent on fields added by demographics processor
# dependent column added after the demographic processor runs
@orca.column("persons_demographics")
def coc(persons):
    return pd.Series(persons.coc_poverty | persons.coc_age,
                     index=persons.index)
