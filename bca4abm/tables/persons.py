import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca
from bca4abm.util.misc import expect_columns


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def raw_bca_persons(data_dir, store, settings):
    return bca.get_raw_table("bca_persons", column_map="bca_persons_column_map")


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def bca_persons(raw_bca_persons):
    return raw_bca_persons.to_frame()


# merge table casting bca_households onto bca_persons
@orca.table()
def bca_persons_merged(bca_persons, bca_households):
    return orca.merge_tables(target=bca_persons.name,
                             tables=[bca_persons, bca_households])


# FIXME - sample of a dependent column that remaps categories
# convert person type categories to string descriptors
@orca.column("bca_persons")
def person_cat(bca_persons, settings):
    return bca_persons.person_type.map(settings["person_type_map"])


# FIXME - sample of a dependent column that remaps categories
# convert person type categories to string descriptors
@orca.column("bca_persons")
def person_gender_cat(bca_persons, settings):
    return bca_persons.person_gender.map(settings["gender_map"])


# FIXME - sample of a dependent column
@orca.column("bca_persons")
def adult(bca_persons):
    return bca_persons.to_frame(["person_age"]).eval("18 <= person_age")


# FIXME - remove this if there are no demographics columns dependent
# this is the placeholder for all the columns to update after the demographic processor runs
@orca.table()
def persons_demographics(bca_persons):
    return pd.DataFrame(index=bca_persons.index)


# FIXME - replace this with any column dependent on fields added by demographics processor
# dependent column added after the demographic processor runs
@orca.column("persons_demographics")
def coc(bca_persons):
    return pd.Series(bca_persons.coc_poverty | bca_persons.coc_age,
                     index=bca_persons.index)
