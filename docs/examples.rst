
.. _examples:

Examples
========

There are two example implementations included in the project, one for an activity-based model and one for
for a four-step (trip-based) model.  bca4abm requires model outputs for a base and a build alternative. The required model 
outputs which are input to bca4abm need to be the same in each alternative. The values 
in the model outputs can be different, but the files, the formats, the number of 
matrices, etc. all need to be the same.  The outputs need to be in open data formats - CSV and `open matrix (OMX) <https://pypi.org/project/OpenMatrix>`_.

Basic Setup
-----------

Each benefits calculator model has the same folder setup:

  * configs - setting and data processor expression files for the overall tool and for each data processor
  * data  - base and build model scenario input data tables and matrices
  * output - an empty output folder
  * run_bca.py - script to run the model

The ``configs`` folder contains settings, expressions files, and other files required for specifying 
model utilities and form.  The first place to start in the ``configs`` folder is ``settings.yaml``, which 
is the main settings file for the model run.  This file includes:

* ``models`` - list of model steps to run - auto demographics processor, person trips processor, etc.
* ``resume_after`` - to resume running the data pipeline after the last successful checkpoint
* ``trace_hh_id`` - trace household id; comment out for no trace
* ``trace_od`` - trace origin, destination pair in accessibility calculation; comment out for no trace
* ``chunk_size`` - batch size for processing choosers
* ``output_tables`` - list of output tables to write to CSV
* ``dump`` - True or False and specifies if bca4abm should write the output intermediate CSV tables for debugging
* global variables that can be used in expressions tables and Python code such as:

    * ``DISCOUNT_RATE`` - cost discounting factor
    * ``ANNUALIZATION_FACTOR`` - daily to annual factor

Included in the ``configs`` folder are the model specification files that store the configuration files for each model. Also included 
in the ``configs`` folder is the ``tables.yaml`` file that stores the file names for each data processor input table,
as well as the mapping from input column names to column names used in the expressions.  The file also specifies the input
table file name and the name of the table used in the expressions.  All fields to be used in expressions must be in the mapping 
since the mapping defines which fields are loaded into memory.

ABM Example 
-----------

Models
~~~~~~

The models are:

  * demographics_processor - disaggregate person calculations
  * person_trips_processor - disaggregate trip calculations
  * auto_ownership_processor - disaggregate person (and household) auto ownership calculations
  * physical_activity_processor - disaggregate person and trip physical activity calculations
  * aggregate_trips_processor - aggregate (matrix-based) calculations
  * link_daily_processor - daily link-based calculations
  * link_processor - time period link-based calculations
  * finalize_abm_results - writes results by equity group (aka COC or community of concern)
  * write_data_dictionary - writes data pipeline data table column types for debugging
  * write_tables - writes output files
  
The current set of files are below.

+------------------------------------------------+--------------------------------------------------------------------+
|            Model                               |    Specification Files                                             |
+================================================+====================================================================+
|  demographics_processor                        |  - demographics.yaml                                               |
|                                                |  - demographics.csv                                                |
+------------------------------------------------+--------------------------------------------------------------------+
|   person_trips_processor                       |  - person_trips.yaml                                               |
|                                                |  - person_trips.csv                                                |
+------------------------------------------------+--------------------------------------------------------------------+
|   auto_ownership_processor                     |  - auto_ownership.yaml                                             |
|                                                |  - auto_ownership.csv                                              |
+------------------------------------------------+--------------------------------------------------------------------+
|   physical_activity_processor                  |  - physical_activity.yaml                                          |
|                                                |  - physical_activity_person.csv                                    |
|                                                |  - physical_activity_trip.csv                                      |
+------------------------------------------------+--------------------------------------------------------------------+
|   aggregate_trips_processor                    |  - aggregate_trips.yaml                                            |
|                                                |  - aggregate_trips.csv                                             |
+------------------------------------------------+--------------------------------------------------------------------+
|   link_daily_processor                         |  - link_daily.yaml                                                 |
|                                                |  - link_daily.csv                                                  |
+------------------------------------------------+--------------------------------------------------------------------+
|   link_processor                               |  - link.yaml                                                       |
|                                                |  - link.csv                                                        |
+------------------------------------------------+--------------------------------------------------------------------+

ABM expression file specifics:
    
    * All target fields specified in the expressions file will be aggregated and written out to the summary results file.  Each summary result entry is named as follows: the two digit processor abbreviation + the target field.  For example: AO_base_auto_ownership_cost. 
    * aggregate_trips.csv references the fields in aggregate_data_manifest.csv as: `base_trips, build_trips, base_ivt, build_ivt, vot, aoc_units, base_aoc, build_aoc, toll_units, base_toll, build_toll`
    * auto_ownership.csv, demographics.csv, physical_activity_person.csv refer to the person table as `persons`
    * link.csv, link_daily.csv refer to the links table as `links`
    * person_trips.csv refers to the trips table as `trips`
    * physical_activity_trip.csv refers to the base and build trips as `trips, base, build`
    
Data
~~~~

The ``data`` folder contains the input data for the base and build scenario.  

  * Disaggregate data

    * persons.csv - base and build alternative person records
    * base_households.csv - base alternative households
    * build_households.csv - build alternative households
    * basetrips_normal.csv - base alternative trips with base alternative trip level-of-service
    * basetrips_buildlos.csv - base alternative trips with build alternative trip level-of-service
    * buildtrips_normal.csv - build alternative trips with build alternative trip level-of-service
    * buildtrips_baselos.csv - build alternative trips with base alternative trip level-of-service

  * Aggregate data

    * aggregate_data_manifest.csv - aggregate markets for which to apply the aggregate calculations defined in aggregate_trips.csv - each row in the manifest contains a trip matrix, an in-vehicle time matrix, an auto operating cost matrix, and a toll matrix.  Each matrix will be loaded for the base and build alternative from the base and build data folders.  These matrices + the additional settings in each row for units, value-of-time, etc. are referenced in the expressions. 
    * base data folder

        * testlos.omx - base alternative level-of-service matrices - IVT, opercost, tollcost
        * testtrips.omx - base alternative trips

    * build data folder        

        * testlos.omx - build alternative level-of-service matrices - IVT, opercost, tollcost
        * testtrips.omx - build alternative trips

  * Link data 

    * link_data_manifest.csv - link time period tables to process in link processor.  The daily like table is not specified since it is handled by a separate daily link processor.
    * base data folder   

      * link_ampk.csv - base alternative AM peak link records
      * link_daily.csv - base alternative daily link records

    * build data folder

      * link_ampk.csv - build alternative AM peak link records
      * link_daily.csv - build alternative daily  link records

Outputs
~~~~~~~

The ABM example outputs are:
  
  * asim.log - log file
  * data_dict.txt - pipeline table data and fields
  * final_summary_results.csv - all calculated measures.  All target fields specified in the expressions file will be aggregated and written out to the summary results file. Each summary result entry is named as follows: the two digit processor abbreviation + the target field. For example: `AO_base_auto_ownership_cost`.
  * final_checkpoints.csv - tables in the pipeline for debugging if desired
  * final_coc_results.csv - results by community of concern
  * final_coc_silos.csv - results by community of concern
  * pipeline.h5.h5 - HDF5 data pipeline which contains all the data tables in pandas format and can be read into pandas with `pd.read_hdf`.

  * If `trace_hh_id` is specified in the settings file:
    
    * auto_ownership.csv - results by person
    * demographics.csv - results by person
    * person_trips.csv - results by person
    * physical_activity_persons.csv - results by person
    * physical_activity_trips.csv - results by trip

Four-Step Example
-----------------

Models
~~~~~~

The models are:

  * aggregate_demographics_processor - zone-based calculations to identify equity groups
  * aggregate_zone_processor - zone-based calculations such as auto ownership and destination choice logsums benefits
  * aggregate_zone_benefits - sum benefits by equity group
  * aggregate_od_processor - OD pair-based calaculations such as travel time savings
  * link_daily_processor - daily link-based calculations such as safety and emissions
  * write_data_dictionary - writes data pipeline data table column types for debugging
  * write_tables - writes output files


The current set of files are below.

+------------------------------------------------+--------------------------------------------------------------------+
|            Model                               |    Specification Files                                             |
+================================================+====================================================================+
|  aggregate_demographics_processor              |  - aggregate_demographics.yaml                                     |
|                                                |  - aggregate_demographics.csv                                      |
+------------------------------------------------+--------------------------------------------------------------------+
|   aggregate_zone_processor                     |  - aggregate_zone.yaml                                             |
|                                                |  - aggregate_zone.csv                                              |
+------------------------------------------------+--------------------------------------------------------------------+
|   aggregate_od_processor                       |  - aggregate_od.yaml                                               |
|                                                |  - aggregate_od.csv                                                |
+------------------------------------------------+--------------------------------------------------------------------+
|   link_daily_processor                         |  - link_daily.yaml                                                 |
|                                                |  - link_daily.csv                                                  |
+------------------------------------------------+--------------------------------------------------------------------+

4Step expression file specifics:
    
    * The `silo` column in the expressions files is used for specifying the relevant communities-of-concern (COC) for the result. An `*` is used to specify that the result applies to everyone.  If the result applies to just one COC, for example, low_income hhs, then the silo entry should correspond to a valid `coc_silos` entry in the setting file, such as `coc_lowinc.`
    * aggregate_demographics.csv refers to the zone table as `cvals`
    * aggregate_zone.csv refers to the zone table as `zones` and prepends `base_` or `build_` 
    * aggregate_od.csv refers to the matrices specified in `aggregate_od_matrices` by name and appends `_base` or `_build` 
    * link.csv refers to the links table specified in the `link_daily_file_names` by `links` plus the name appended

Data
~~~~

The ``data`` folder contains the input data for the base and build scenario.  

  * Link
    
    * linksMD1.csv - link MD1 period assignment results 
    * linksPM2.csv - link PM2 assignment results
   
  * OD  
    
    * assign_mfs.omx - assignment matrices
    * skims_mfs.omx - skims matrices
    * mode_choice_pa.omx - mode choice production-attraction matrices
    * parking_cost.omx - parking costs at the destination
   
  * Zone 
    
    * mf.cval.csv - see above
    * cocs.csv - externally defined COC share of households by zone
    * Productions files such as ma.hboprh.csv (hbo high inc)
    * Destination choice logsums files such as ma.hbohdcls.csv (hbo high inc)

  * zone_districts.csv - zone district scheme for district-to-district O-D processor aggregate report and for zone labels if zones non-sequential
  * moves_2010_summer_running_rates.csv - EPA MOVES emissions rate table lookup file

Outputs
~~~~~~~

The 4step example outputs are:
  
  * bca.log - log file
  * data_dict.txt - pipeline table data and fields
  * final_aggregate_results.csv - results by measure and COC, including for everyone
  * pipeline.h5.h5 - HDF5 data pipeline which contains all the data tables in pandas format and can be read into pandas with `pd.read_hdf`.
  * Intermediate outputs for debugging
  
    * final_zone_demographics.csv - demographics processor calculated fields for each zone 
    * final_aggregate_zone_summary.csv - zone processor calculated fields for each zone 
    * final_aggregate_od_zone_summary.csv - OD processor calculated fields summed to each origin zone 
    * final_aggregate_od_district_summary.csv - District-to-district OD processor calculated fields summed 
    * link_daily_results_base|build.csv - link processor calculated fields
    
  * If `trace_od` is specified in the settings file:
    
    * aggregate_demographics.csv - demographics processor calculated fields for the trace origin zone 
    * aggregate_zone.csv - zone processor calculated fields for the trace origin zone 
    * aggregate_od.csv - OD processor calculated fields for the trace OD pair 
    * link_daily_results_build.csv - build scenario link processor calculated fields for links in the trace origin or destination zone 
    * link_daily_results_base.csv - base scenario link processor calculated fields for links in the trace origin or destination zone 
