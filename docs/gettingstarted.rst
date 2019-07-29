
Getting Started
===============

bca4abm includes activity-based model and four-step trip-based model :ref:`examples` to help get you started.

Installation
------------

* Install `Anaconda 64bit Python 3 <https://www.anaconda.com/distribution/>`__, which includes a number of required Python packages.
* Create and activate an Anaconda environment (i.e. a Python install just for this project).

::

  conda create -n bca4abmtest python=3.7
  activate bca4abmtest

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

* Check the outputs folder for results, for example the ``final_aggregate_results.csv`` file for the 4step example

Process Overview
----------------

The basic steps to run the benefits calculator with your travel model are below:

  * Run procedures to export results from the travel model to produce the base and build scenario inputs required for the calculator
  * The calculator reads the travel model output files for a base and build scenario
  * The calculator evaluates user-defined Python expressions (see below) for each data processor to calculate benefits.  Expressions are segmented by equity group when applicable, and include subtracting the base from the build quantity, monetization, annualization, etc.
  * The summation of the calculations by benefit and equity group are written out

Expressions
-----------

To help illustrate how the benefits calculator works, an example set of expressions for calculating zone benefits is below.  Each input is a zone data table with
each row a zone and each column a zone attribute.  The example processes the
base and build home-based-other productions by zone, as well as the base and build mode choice logsum, and calculates
an accessibility benefit measure.  The idea here is that improvements in multi-modal accessibility (i.e. the logsum) between the
based and build scenario results in increased accessibility or additional travel options.  The accessibility benefit is calculated
using the rule-of-half.  As a result, the calculation is half the difference in the productions
times the difference in the logsums divided by utilities per minute for home-based-other trips times the value-of-time for
home-based-other trips times the annual discount rate times a daily to annual factor.  The result is monetized benefits for increases
in accessibility by zone.

+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  Description                              | Target                 | Expression                                                                                                                                 |
+===========================================+========================+============================================================================================================================================+
|  #zone input data tables                  |                        |                                                                                                                                            |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  hbo productions in base scenario         |  base_prod_hbo         |  zones.base_hboprl + zones.base_hboprm + zones.base_hboprh                                                                                 |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  hbo productions in build scenario        |  build_prod_hbo        |  zones.build_hboprl + zones.build_hboprm + zones.build_hboprh                                                                              |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  hbo logsum in base scenario              |  base_ls_hbo           |  zones.base_hbodcls                                                                                                                        |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  hbo logsum in build scenario             |  build_ls_hbo          |  zones.build_hbodcls                                                                                                                       |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  #calculate travel options benefit by zone|                        |                                                                                                                                            |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  access benefit hbo                       |  access_benefit_hbo    |  (0.5 * (base_prod_hbo + build_prod_hbo) * (build_ls_hbo - base_ls_hbo) / UPM_HBO) * (VOT_HBO / 60) * DISCOUNT_RATE * ANNUALIZATION_FACTOR |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  #add up all travel purposes if calculated|                        |                                                                                                                                            |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
|  travel options benefit                   |  travel_options_benefit|  access_benefit_hbo + access_benefit_hbr + access_benefit_hbs + access_benefit_hbw + access_benefit_nhbnw + access_benefit_nhbw            |
+-------------------------------------------+------------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
