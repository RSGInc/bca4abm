# %load_ext autoreload
# %autoreload 2

# cd someplace where you don't mind creating an output dir
# contents of this directory are .gitignored
# cd tutorials/data

import orca
import pandas as pd
import os
from activitysim import defaults
from activitysim import activitysim as asim


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
    'age': [14, 104, 3, 82, 7 ],
     'iq': [100, 140, 87, 120, 94],
     'species_id': ['D', 'D', 'C', 'C', 'D']},
    index=['p0', 'p1', 'p2', 'p3', 'p4'])

df_pet['init_age'] = df_pet['age']
df_pet

# register the tables
orca.add_table('species', df_species)
orca.add_table('pets', df_pet)

# broadcast so we can merge pets and species dataframes on pets.species_id
orca.broadcast(cast='species', onto='pets', cast_index=True, onto_on='species_id')

# this is a common merge so might as well define it once here and use it
@orca.table()
def pets_merged(pets, species):
    return orca.merge_tables(pets.name,
        tables=[pets, species])

# this is the orca registered version of the merged table
orca.get_table('pets_merged').to_frame()


##########################################################################
# simple_simulate
##########################################################################

# register our spec table (customarily stored in configs dir)
# (configs_dir predefined as '.' by injectable in activitysim.defaults.misc.py)
@orca.injectable()
def pet_spec(configs_dir):
    # Note - odd semantics in activitysim where configs_dir is really configs_parent_dir
    f = os.path.join(configs_dir, 'configs', "pet_activity.csv")
    return asim.read_model_spec(f).fillna(0)

orca.eval_variable('pet_spec')


# step-by-step illustration of what simple_simulate does behind the scenes
@orca.step()
def pet_activity_simple_simulate(set_random_seed, pets_merged, pet_spec):

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    choosers = pets_merged.to_frame()

    # table of variable specifications and coefficient values of alternatives (a pandas.DataFrame)
    spec = pet_spec

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d=None

    # eval_variables evaluates each of the expressions in spec
    # in the context of each row in of the choosers dataframe
    model_design = asim.eval_variables(spec.index, choosers, locals_d)

    print "\n### model_design - results of the expressions for each row in choosers"
    print model_design

    utilities = model_design.dot(spec)

    print "\n### utilities - the net utility of each alternative for each row in choosers"
    print utilities

    probs = asim.utils_to_probs(utilities)

    print "\n### probs - utilities normalized as relative choice probablities (summing to 1)"
    print probs

    # Make choices for each chooser from among the set of alternatives based on probability
    choices = asim.make_choices(probs)

    print "\n### choices - choices expressed as zero-based index into set of alternatives"
    print choices

    # simple_simulate returns two dataframes: choices and model_design

orca.run(["pet_activity_simple_simulate"])

# example of how simple_simulate based models work in activitysim (e.g. auto_ownership)
@orca.step()
def pet_activity_simulate(set_random_seed, pets_merged, pet_spec):

    # the choice model will be applied to each row of the choosers table (a pandas.DataFrame)
    choosers = pets_merged.to_frame()

    # pandas.DataFrame table of variable specifications and coefficient values of alternatives
    spec = pet_spec

    # locals whose values will be accessible to the execution context
    # when the expressions in spec are applied to choosers
    locals_d=None

    choices, model_design = asim.simple_simulate(choosers, spec)

    print "\n### model_design - results of the expressions for each row in choosers"
    print model_design

    print "\n### choices - choices expressed as zero-based index into set of alternatives"
    print choices

    # convert choice indexes to spec column names
    activity_names = spec.columns.values
    choice_names = choices.apply(lambda x : activity_names[x])

    print "\n### choice_names - choices expressed as names of columns of alternatives in spec"
    print choice_names

    # store the results so they are available to subsequent models, etc
    orca.add_column("pets", "pet_activity", choices)
    orca.add_column("pets", "pet_activity_names", choice_names)

orca.run(["pet_activity_simulate"])

# pets_merged with pet_activity and pet_activity_names columns assigned by pet_activity_simulate
orca.get_table('pets_merged').to_frame()

# note that the new columns were added to the orca.DataFrameWrapper table, not the df_pet dataframe
df_pet

##########################################################################
# a short discursion on the add_dependent_columns idiom common in activitysim.models
##########################################################################

# this is a placeholder table for columns that get computed after the
# pet activity model
@orca.table()
def households_pet_activity(pets):
    return pd.DataFrame(index=pets.index)

# this is a dependent column we want to add after running pet_activity_simulate
@orca.column('households_pet_activity')
def sleeping(pets):
    return (pets.pet_activity == 2)

# use of this (hidden) utility function is a common idiom in activitysim.defaults
# from activitysim.defaults.models.utils.misc import add_dependent_columns
def add_dependent_columns(base_dfname, new_dfname):
    tbl = orca.get_table(new_dfname)
    for col in tbl.columns:
        print "Adding", col
        orca.add_column(base_dfname, col, tbl[col])

# this will copy the values in at the time it is applied
add_dependent_columns("pets", "households_pet_activity")

# this one will recalc when pet_activity changes
@orca.column('pets')
def recalc_sleeping(activity='pets.pet_activity'):
    return activity==2


##########################################################################
# orca run with iteration
##########################################################################

# create a dataframe to hold the results of the iterations
df_history = pd.DataFrame(columns=['pet_name', 'timestamp', 'age', 'pet_activity_names'])
orca.add_table('history', df_history)


# create a step to age pets at a rate appropriate to their species
@orca.step()
def age_simulate(pets):
    new_age = pets.age + orca.get_table('pets_merged').age_rate
    pets.update_col_from_series('age', new_age)


# create a step to checkpoint iteration values
@orca.step()
def summarize(pets_merged, history, iter_var):

    pets_row = pets_merged.to_frame()[['pet_name', 'age', 'pet_activity_names']]
    pets_row['timestamp'] = iter_var

    df = history.to_frame().append(pets_row, ignore_index=True)
    orca.add_table(history.name, df)


# now lets run thrree steps as an orca pipeline, iterated over a range
orca.run(['pet_activity_simulate', 'age_simulate', 'summarize'], iter_vars=range(2010, 2020))

# this is the history file we wrote out while iterating
history = orca.get_table('history').to_frame()
history

# the history table pivoted with counts by year of activities across pets
pd.pivot_table(history, index='timestamp',
               columns=['pet_activity_names'], values='age', fill_value=0, aggfunc='count')

# the update ages are are visible in the orca registered pets_merged table
orca.get_table('pets_merged').to_frame()[['pet_name', 'init_age', 'age']]

# because we used orca.DataFrameWrapper.update_col_from_series to update age,
# it wrote through to the underlying df_pets dataframe
# if you are mixing dataframe access within and outside orca,
# you need to be mindful of how orca references tables
df_pet

# to re-run, we need to reinitialize tables changed by the simulation
df_pet['age'] = df_pet['init_age']
orca.add_table('pets', df_pet)
orca.add_table('history', df_history)

