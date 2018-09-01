bca4abm
=======

bca4abm is an open platform for benefit-cost analysis for comparing scenarios generated
by travel demand models - activity-based or trip-based.  It is a general framework for 
calculating benefits since it works with a variety of input data types:

  * zones
  * O-D pairs
  * links
  * persons
  * trips

bca4abm is implemented in the `ActivitySim framework <https://github.com/activitysim/activitysim>`_. 

bca4abm exposes most of its calculations in CSV files that contain numpy and 
pandas expression that operate on the input data tables and matrices. This avoid 
having to modify Python code when making changes to the model calculations. An 
example set of expressions is in the auto_ownership expression file - auto_ownership.csv. 
Refer to :ref:`Expressions` for more info.

bca4abm requires model outputs for both a base and a build alternative. The required model 
outputs which are input to bca4abm need to be the same in each alternative. The values 
in the model outputs can be different, but the files, the formats, the number of 
matrices, etc. all need to be the same.

Contents
--------

.. toctree::
   :maxdepth: 2

   gettingstarted

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

