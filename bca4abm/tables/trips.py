import numpy as np
import orca
import pandas as pd


@orca.table(cache=True)
def trips(set_random_seed, store, settings):

    return store["trips"]
