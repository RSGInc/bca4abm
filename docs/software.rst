
Software Implementation 
=======================

This page describes the bca4abm software implementation and how to contribute.  

The implementation starts with 
the ActivitySim framework, which serves as the foundation for the software.  The framework, as briefly described
below, includes features for data pipeline management, expression handling, testing, etc.  Built upon the 
framework are additional core components for benefits calculation.

ActivitySim Framework
---------------------

bca4abm is implemented in the `ActivitySim <https://github.com/activitysim/activitysim>`__ 
framework.  As summarized `here <https://activitysim.github.io/activitysim/#software-design>`__, 
being implemented in the ActivitySim framework means:

* Overall Design

  * Implemented in Python, and makes heavy use of the vectorized backend C/C++ libraries in `pandas <http://pandas.pydata.org>`__ and `numpy <http://www.numpy.org>`__.
  * Vectorization instead of for loops when possible
  * Runs sub-models that solve Python expression files that operate on data tables
  
* Data Handling

  * Inputs are in CSV format, with the exception of settings
  * CSVs are read-in as pandas tables and stored in an intermediate HDF5 binary file that is used for data I/O throughout the model run
  * Key outputs are written to CSV files
  
* Key Data Structures

  * `pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`__ - A data table with rows and columns, similar to an R data frame, Excel worksheet, or database table
  * `pandas.Series <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.Series.html>`__ - a vector of data, a column in a DataFrame table or a 1D array
  * `numpy.array <https://docs.scipy.org/doc/numpy/reference/generated/numpy.array.html>`__ - an N-dimensional array of items of the same type, such as a matrix
  
* Model Orchestrator

  * `ORCA <https://github.com/UDST/orca>`__ is used for running the overall model system and for defining dynamic data tables, columns, and injectables (functions). ActivitySim wraps ORCA functionality to make a Data Pipeline tool, which allows for re-starting at any model step.  
    
* Expressions

  * Model expressions are in CSV files and contain Python expressions, mainly pandas/numpy expression that operate on the input data tables. This helps to avoid modifying Python code when making changes to the model calculations. 
    
* `Code Documentation <https://activitysim.github.io/activitysim/development.html>`__

  * Python code according to `pycodestyle <https://pypi.python.org/pypi/pycodestyle>`__ style guide
  * Written in `reStructuredText <http://docutils.sourceforge.net/rst.html>`__ markup, built with `Sphinx <http://www.sphinx-doc.org/en/stable>`__ and docstrings written in `numpydoc <https://github.com/numpy/numpy/blob/master/doc/HOWTO_DOCUMENT.rst.txt>`__
    
* `Testing <https://activitysim.github.io/activitysim/development.html>`__

  * A protected master branch that can only be written to after tests have passed
  * `pytest <https://docs.pytest.org/en/latest/>`__ for tests
  * `TravisCI <https://travis-ci.org>`__ for building and testing with each commit

Common
------

Software components common to both ABM and four-step model usage.

bca4abm
^^^^^^^

.. automodule:: bca4abm.bca4abm
   :members:
   
aggregate_trips
^^^^^^^^^^^^^^^

.. automodule:: bca4abm.processors.aggregate_trips
   :members:

link
^^^^

.. automodule:: bca4abm.processors.link
   :members:
   
ABM Processors
--------------

Software components for ABM model usage.

abm_results
^^^^^^^^^^^

.. automodule:: bca4abm.processors.abm.abm_results
   :members:
   
auto_ownership
^^^^^^^^^^^^^^

.. automodule:: bca4abm.processors.abm.auto_ownership
   :members:
   
demographics
^^^^^^^^^^^^

.. automodule:: bca4abm.processors.abm.demographics
   :members:
   
person_trips
^^^^^^^^^^^^

.. automodule:: bca4abm.processors.abm.person_trips
   :members:

physical_activity
^^^^^^^^^^^^^^^^^

.. automodule:: bca4abm.processors.abm.physical_activity
   :members:

Four Step Processors
--------------------

Software components for four-step model usage.

aggregate_demographics
^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: bca4abm.processors.four_step.aggregate_demographics
   :members:
   
aggregate_zone
^^^^^^^^^^^^^^

.. automodule:: bca4abm.processors.four_step.aggregate_zone
   :members:
   
aggregate_od
^^^^^^^^^^^^

.. automodule:: bca4abm.processors.four_step.aggregate_od
   :members:
   
Contribution Guidelines
-----------------------

bca4abm development follows the same `development guidelines <https://activitysim.github.io/activitysim/development.html>`__ as ActivitySim.


Release Notes
-------------

  * v0.4 - first release