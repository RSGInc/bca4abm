
Getting Started
===============

Examples
--------

There are two example implementations included in this repository: ABM and 4step (trip-based).

ABM models defined in `settings.yaml <https://github.com/RSGInc/bca4abm/blob/master/example_abm/configs/settings.yaml>`_

  * demographics_processor - disaggregate person calculations
  * person_trips_processor - disaggregate trip calculations
  * auto_ownership_processor - disaggregate person (and household) auto ownership calculations
  * physical_activity_processor - disaggregate person and trip physical activity calculations
  * aggregate_trips_processor - aggregate (matrix-based) calculations
  * link_daily_processor - daily link-based calculations
  * link_processor - time period link-based calculations
  * write_tables - writes output files

4step models defined in `settings.yaml <https://github.com/RSGInc/bca4abm/blob/master/example_4step/configs/settings.yaml>`_

  * aggregate_demographics_processor - zone-based calculations to identify communities of concern (COC) / market segments
  * aggregate_zone_processor - zone-based calculations such as auto ownership and destination choice logsums benefits
  * aggregate_od_processor - OD pair-based calaculations such as travel time savings
  * link_daily_processor - daily link-based calculations such as safety and emissions
  * write_tables - writes output files

Installation
------------

* Install `Anaconda Python 2.7 <https://www.continuum.io/downloads>`_, which includes a number of required Python packages.
* Create and activate an Anaconda environment (i.e. a Python install just for this project).

:: 

  conda create -n bca4abmtest python=2.7
  activate bca4abmtest

* Get and install other required libraries from the `Python Package Index <https://pypi.python.org/pypi>`_

:: 

  pip install orca toolz zbox openmatrix activitysim

* Get and install the bca4abm package from `GitHub <https://github.com/RSGInc/bca4abm>`_

:: 

  pip install https://github.com/RSGInc/bca4abm/zipball/master


Running the Model
-----------------

* Activate the conda Python environment

:: 

  activate bca4abmtest

* Change to the ABM or 4step example folder and then run the run_bca.py program

::

  python run_bca.py

* Check the outputs folder for results