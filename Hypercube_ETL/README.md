# Hypercube Consulting Data Engineer Technical Test

## Aim
This exercise aims to assess the candidate across a broad range of typical data engineer tasks. The candidate is expected to design and code solutions to the tasks outlined below, using the provided data. 

Candidates should have an environment that allows them to code functions and unit tests for transforming and loading data, and building APIs - this could either be scripts or notebooks.

Part B will have architectural and design questions, the candidate may find it useful to use a diagramming or graphing tool ready to explain their thinking - again this is not essential.

Please take no more than three hours on this take-home test, it is not expected that candidates complete it. If the code is taking longer, please park at a sensible stopping point. 

**NOTE: Candidates are not expected to build a working production system for the purposes of this exercise. They may do so if it aids their thinking but they will be assessed on the quality of their code, diagrams, and responses to the tasks outlined below. Hypercube's aim is to assess the candidate's engineering ability and their approach to problem solving.**

All external dependencies can be mocked for speed and brevity.

## Data
The data consists of two files, located in the ``/data`` folder.
   - ``linear_orders_raw.json``
   - ``bmrs_wind_forecast_pair.csv``

You can use NESO (https://www.neso.energy/data-portal/dynamic-containment-data/dc_dr__dm_linear_orders_master_data_2021-2023) and similar resources for more information on the datasets.

## Tasks
### Part A
Write code and tests for a multi-stage pipeline to achieve the following:

1. Assessing the data quality of and cleaning ``bmrs_wind_forecast_pair.csv`` and imputing ``NULL`` values for numerical columns
2. Joining bmrs_wind_forecast_pair.csv to ``linear_orders_raw.json`` on ``DeliveryStart``
3. Build the feature engineering transformation for a simple time series forecasting model:
    - for the `initialForecastSpnGeneration` and `ExecutedVolume` fields calculate rolling medians over a 6-hour window
    - build a daily and weekly aggregate view of the data
4. Devise a database schema to hold the data and apply it to a MySQL or SQLite database

(**Stretch - complete this after part B only if you have ample time**)

5. Create a Docker image for loading the joined data into the tables you have created in the database. Make sure the appropriate config is in the docker-compose file.
6. Simulate persisting your data: 
    - Mock a REST call to PUT the output from 2. to a fictional API end-point ``https://hypercube.ingestion.com/wind-orders`` and write to CSV

### Part B
Discuss the following questions:

1. How would you do this in a production cloud environment?
2. How would your solution change if the data inputs were to be received in the form of streamed messages?
3. How would your solution change if the data was 100x the scale?
4. What kind of architecture and approach would you use to serve this data for dashboards and reporting, used by multiple teams with differing requirements?