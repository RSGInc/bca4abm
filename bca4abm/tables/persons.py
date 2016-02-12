import os.path
import numpy as np
import orca
import pandas as pd

from bca4abm import bca4abm as bca


# this caches things so you don't have to read in the file from disk again
@orca.table(cache=True)
def bca_persons_internal(data_dir, store, settings):

    if "bca_persons" in settings:
        df = bca.read_bca_table("bca_persons", 'person_id', data_dir, settings)
    else:
        df = store["bca_persons"]

    bca.expect_columns(df, ["hh_id",
                            "person_type",
                            "person_age",
                            "person_gender"])

    return df


# this caches all the columns that are computed on the persons table
@orca.table(cache=True)
def bca_persons(bca_persons_internal):
    return bca_persons_internal.to_frame()


@orca.table()
def bca_persons_merged(bca_persons, bca_households):
    return orca.merge_tables(target=bca_persons.name,
                             tables=[bca_persons, bca_households])


# convert person type categories to string descriptors
@orca.column("bca_persons")
def person_cat(bca_persons, settings):
    return bca_persons.person_type.map(settings["person_type_map"])


# convert person type categories to string descriptors
@orca.column("bca_persons")
def person_gender_cat(bca_persons, settings):
    return bca_persons.person_gender.map(settings["gender_map"])


@orca.column("bca_persons")
def adult(bca_persons):
    return bca_persons.to_frame(["person_age"]).eval("18 <= person_age")
