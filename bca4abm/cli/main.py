# bca4abm
# Copyright (C) 2016 RSG Inc
# See full license in LICENSE.txt.
"""
Welcome to bca4abm!
"""

import os
import shutil
import sys
import warnings
import logging
import argparse

from activitysim.core import tracing
from activitysim.core import pipeline
from activitysim.core import inject

from activitysim.core.config import setting

# the following import has the side-effect of registering injectables
import bca4abm


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', action='version', version=bca4abm.__version__)

    # print help if no subcommand is provided
    parser.set_defaults(func=lambda x: parser.print_help())
    subparsers = parser.add_subparsers(title='subcommands',
                                       help='available subcommand options')

    subparser_create(subparsers)
    subparser_run(subparsers)

    args = parser.parse_args()
    args.func(args)


def subparser_create(subparsers):
    """Create command args
    """
    parser_create = subparsers.add_parser('create', description=create.__doc__,
                                          help='create a new bca4abm configuration')
    create_group = parser_create.add_mutually_exclusive_group(required=True)
    create_group.add_argument('-l', '--list',
                              action='store_true',
                              help='list available example directories')
    create_group.add_argument('-e', '--example',
                              type=str,
                              metavar='PATH',
                              help='example directory to copy')
    parser_create.add_argument('-d', '--destination',
                               type=str,
                               metavar='PATH',
                               default=os.getcwd(),
                               help="path to new project directory (default: %(default)s)")
    parser_create.set_defaults(func=create)


def create(args):
    """
    Create a new bca4abm configuration from an existing template.

    Use the -l flag to view a list of example configurations, then
    copy to your own working directory. These new project files can
    be run with the 'bca run' command.
    """

    # FIXME: change this to generic root package directory
    root_dir = os.path.abspath(os.path.join(__file__, '../../..'))
    example_dirs = [
        item for item in os.listdir(root_dir)
        if os.path.isdir(os.path.join(root_dir, item))
        and 'example' in item
    ]

    if args.list:
        print('Available examples:')
        for example in example_dirs:
            print("\t"+example)

        sys.exit(0)

    if args.example:
        if args.example not in example_dirs:
            sys.exit("error: could not find example '%s'" % args.example)

        if os.path.isdir(args.destination):
            dest_path = os.path.join(args.destination, args.example)
        else:
            dest_path = args.destination

        example_path = os.path.join(root_dir, args.example)

        print('copying files from %s...' % args.example)
        shutil.copytree(example_path, dest_path)

        sys.exit("copied! new project files are in %s" % os.path.abspath(dest_path))


def subparser_run(subparsers):
    """Run command args
    """
    parser_run = subparsers.add_parser('run', description=run.__doc__, help='run bca4abm')
    parser_run.add_argument('project_dir', type=str, help='path to example/project directory')
    parser_run.add_argument('-c', '--config', type=str, help='path to config dir')
    parser_run.add_argument('-o', '--output', type=str, help='path to output dir')
    parser_run.add_argument('-d', '--data', type=str, help='path to data dir')
    parser_run.add_argument('-r', '--resume', type=str, help='resume after step')
    parser_run.add_argument('-p', '--pipeline', help='pipeline file name')
    parser_run.set_defaults(func=run)


def run(args):
    """
    Run bca4abm on an existing project folder.

    This command will read the settings/configs found in the
    configs directory of your project folder.
    """

    if not os.path.exists(args.project_dir):
        sys.exit("error: could not find project folder '%s'" % args.project_dir)

    os.chdir(args.project_dir)

    if args.config:
        if not os.path.exists(args.config):
            sys.exit("Could not find configs dir '%s'" % dir)
        inject.add_injectable('configs_dir', args.config)

    if args.data:
        if not os.path.exists(args.data):
            sys.exit("Could not find data dir '%s'" % dir)
        inject.add_injectable('data_dir', args.data)

    if args.output:
        if not os.path.exists(args.output):
            sys.exit("Could not find output dir '%s'." % args.output)
        inject.add_injectable('output_dir', args.output)

    if args.pipeline:
        inject.add_injectable('pipeline_file_name', args.pipeline)

    if args.resume:
        override_setting('resume_after', args.resume)

    tracing.config_logger()
    tracing.delete_csv_files()  # only modifies output_dir
    warnings.simplefilter('always')
    logging.captureWarnings(capture=True)

    t0 = tracing.print_elapsed_time()

    # If you provide a resume_after argument to pipeline.run
    # the pipeline manager will attempt to load checkpointed tables from the checkpoint store
    # and resume pipeline processing on the next submodel step after the specified checkpoint
    resume_after = setting('resume_after', None)

    if resume_after:
        print('resume_after: %s' % resume_after)

    pipeline.run(models=setting('models'), resume_after=resume_after)

    # tables will no longer be available after pipeline is closed
    pipeline.close_pipeline()

    t0 = tracing.print_elapsed_time('all models', t0)


# TODO: move to activitysim.core.config
def override_setting(key, value):
    new_settings = inject.get_injectable('settings')
    new_settings[key] = value
    inject.add_injectable('settings', new_settings)


if __name__ == '__main__':
    sys.exit(main())
