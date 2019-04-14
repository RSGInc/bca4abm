bca4abm
=======

bca4abm is an open platform for benefit-cost analysis for comparing scenarios generated
by travel demand models - activity-based or trip-based.  It is a general framework for 
calculating benefits based on a variety of input data types:

  * zones
  * O-D pairs
  * links
  * persons
  * trips

bca4abm is implemented in the `ActivitySim framework <https://github.com/activitysim/activitysim>`_ and
therefore exposes its calculations in CSV files that contain pandas and numpy 
expressions that operate on the input data tables and matrices. This design avoids 
having to modify Python code when making changes to the benefit calculations. 
Users are recommended to refer to the ActivitySim documentation for more information.

Benefits
--------

Example activity-based model benefits that have been calculated with the benefits calculator are:
  
  * travel time and cost reductions by person and equity group
  * travel options (logsums accessibility) improvements by person trip and equity group
  * vehicle ownership cost reductions by household and equity group
  * physical activity improvements by person and equity group   
  * aggregate trip model (truck, visitor, airport) time and cost reductions by market
  * highway safety improvements by link
  * vehicle operating cost reductions by link
  * travel time reliability improvements by link

Example trip-based model benefits that have been calculated with the benefits calculator are:

  * travel time and cost reductions by O-D pair and equity group
  * travel time reliability improvements by O-D pair and equity group
  * physical activity improvements by O-D pair
  * vehicle ownership cost reductions by zone and equity group
  * travel options (logsums accessibility) improvements by zone and equity group
  * highway safety improvements by link
  * emissions reductions by link
  * surface water runoff reductions by link
  * highway noise reductions by link
  * vehicle operating cost reductions by link

The tool is especially useful for auditing results from travel models.  The benefits calculator provides 
a tremendous amount of information that is converted into easily understandable units such as 
minutes and dollars.  This helps to illuminate model system components, relationships, and their sensitivities.


Contents
--------

.. toctree::
   :maxdepth: 3

   gettingstarted
   examples
   resources

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

