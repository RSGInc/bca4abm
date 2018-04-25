# bca4abm
# See full license in LICENSE.txt.

import logging
import os
import pandas as pd

from activitysim.core import inject
from activitysim.core import pipeline

logger = logging.getLogger(__name__)

@inject.step()
def finalize_abm_results(coc_results, summary_results, settings):

    data_dict = inject.get_injectable('data_dictionary')

    # - coc_silos
    coc_silos = pd.DataFrame()
    if coc_results.index.names != [None]:

        coc_column_names = coc_results.index.names
        assigned_column_names = coc_results.columns
        df = coc_results.to_frame().reset_index(drop=False)

        # one column for each coc column
        for coc in coc_column_names:
            coc_silos[coc] = df[df[coc] == True][assigned_column_names].sum()
        # plus an extra column for 'any coc'
        coc_silos['any_coc'] = df[df[coc_column_names].any(axis=1)][assigned_column_names].sum()
        coc_silos.sort_index(inplace=True)

        coc_silos.index.name = 'Target'
        coc_silos.reset_index(inplace=True)

        # add the description from the data dictionary
        coc_silos['Description'] = coc_silos['Target'].map(data_dict).fillna('')

    pipeline.replace_table('coc_silos', coc_silos)

    # - coc_results no need to reset multi-index so column names will print
    # coc_results = coc_results.to_frame()
    # coc_results.reset_index(inplace=True)
    # pipeline.replace_table('coc_results', coc_results)

    # - transpose and annotate summary_results
    summary_results_t = summary_results.to_frame().T
    summary_results_t.sort_index(inplace=True)
    summary_results_t.reset_index(inplace=True)
    summary_results_t.columns = ['Target', 'Value']
    # add the description from the data dictionary
    summary_results_t['Description'] = summary_results_t.Target.map(data_dict).fillna('')

    pipeline.replace_table('summary_results', summary_results_t)
