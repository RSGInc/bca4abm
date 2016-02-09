##########################################################################
# autoreload
##########################################################################
# %load_ext autoreload
# %autoreload 2

##########################################################################
# orca basics
##########################################################################
import orca
import pandas as pd
import numpy as np
import os


##########################################################################
# registering and merging tables
##########################################################################

df_species = pd.DataFrame(
    {'species_name': ['dog', 'cat'],
    'age_rate': [7, 5]},
    index=['D', 'C'])

df_species


df_pet = pd.DataFrame(
    {'pet_name': ['wilkie', 'lassie', 'leo', 'felix', 'rex' ],
    'age': [0, 0, 0, 0, 0 ],
     'species_id': ['D', 'D', 'C', 'C', 'D']},
    index=['p0', 'p1', 'p2', 'p3', 'p4'])

df_pet

# register the tables
orca.add_table('species', df_species)
orca.add_table('pets', df_pet)

# broadcast merge
orca.broadcast(cast='species', onto='pets', cast_index=True, onto_on='species_id')

### this is the merged table
orca.merge_tables(target='pets',
                  tables=['pets', 'species'],
                  columns=['pet_name', 'age', 'species_name', 'age_rate'])

# this is a common merge so might as well define it once here and use it
@orca.table()
def pets_merged(pets, species):
    return orca.merge_tables(pets.name,
        tables=[pets, species],
        columns=['pet_name', 'age', 'species_name', 'age_rate'])

# this is the orca registered version of the merged table
orca.get_table('pets_merged').to_frame()

# eval_variable is a debugging tool to eval tables, functions and other injectables
with orca.injectables(x=1, y=2):
    df = orca.eval_variable('pets_merged', pets=df_pet, species=df_species).to_frame()
df

##########################################################################
# pipelining
##########################################################################

# create a step to age pets at a rate appropriate to their species
@orca.step()
def age_simulate(pets):
    new_age = pets.age + orca.get_table('pets_merged').age_rate
    pets.update_col_from_series('age', new_age)

# create a second step to illustrate how pipelining works
@orca.step()
def summarize(pets, iter_var):
    print '*** i = {} ***'.format(iter_var)
    print pets.to_frame()[['pet_name', 'age']]


# now lets run an orca pipeline

# data_out (optional) is the filename of pandas HDF data store
# to which all tables injected into any step will be saved
hdf_output_filename = '../output/run.h5'
orca.run(['age_simulate', 'summarize'], iter_vars=range(2010, 2015), data_out=hdf_output_filename)

# lets inspect the output
store = pd.HDFStore(hdf_output_filename)


# <class 'pandas.io.pytables.HDFStore'>
# File path: ./output/run.h5
# /2011/pets            frame        (shape->[5,3])
# /2012/pets            frame        (shape->[5,3])
# /2013/pets            frame        (shape->[5,3])
# /2014/pets            frame        (shape->[5,3])
# /base/pets            frame        (shape->[5,3])

store['/base/pets']

print "--------"
print store['/2014/pets']

store.close()


###########

