#! /usr/bin/env bash

# Exit on error
set -e

echo "Installing dependencies"

#source activate bca

#conda install --yes --quiet sphinx numpydoc
#pip install sphinx_rtd_theme

#pip install sphinx numpydoc sphinx_rtd_theme
pip freeze

echo "Building docs"
cd docs
make clean
make html
