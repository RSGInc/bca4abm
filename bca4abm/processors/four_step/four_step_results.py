import os
import orca
import pandas as pd


@orca.step()
def write_four_step_results(output_dir, aggregate_results, summary_results, settings):

    print "---------- write_four_step_results"

    df = aggregate_results.to_frame()

    with orca.eval_variable('output_store') as output_store:
        output_store['aggregate_results'] = df

    if settings.get("dump", False):

        csv_file_name = os.path.join(output_dir, 'aggregate_results.csv')
        print "writing", csv_file_name
        df.to_csv(csv_file_name, index=False)
