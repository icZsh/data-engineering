-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi_rides_ny.yellow_taxi_data`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_hw3_2025_isaac/yellow_tripdata_2024-*.parquet']
);

-- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records 
CREATE OR REPLACE TABLE taxi_rides_ny.yellow_taxi_data_non_partitoned AS
SELECT * FROM taxi_rides_ny.yellow_taxi_data;

-- Count of records for the 2024 Yellow Taxi Data
SELECT COUNT(*) FROM `taxi_rides_ny.yellow_taxi_data_non_partitoned`;

-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables
SELECT COUNT(DISTINCT PULocationID) loc_cnts FROM taxi_rides_ny.yellow_taxi_data;
SELECT COUNT(DISTINCT PULocationID) loc_cnts FROM taxi_rides_ny.yellow_taxi_data_non_partitoned;

-- Retrieve the PULocationID from the table
SELECT PULocationID 
FROM taxi_rides_ny.yellow_taxi_data_non_partitoned;

-- Retrieve the PULocationID and DOLocationID on the same table
SELECT PULocationID, DOLocationID 
FROM taxi_rides_ny.yellow_taxi_data_non_partitoned;

-- Queries with a fare amount of zero
SELECT COUNT(VendorID) zero_fare_trips FROM taxi_rides_ny.yellow_taxi_data_non_partitoned
WHERE fare_amount = 0;

-- Create a partition and cluster table
CREATE OR REPLACE TABLE taxi_rides_ny.yellow_taxi_data_partitoned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM taxi_rides_ny.yellow_taxi_data;

-- Retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
SELECT COUNT(DISTINCT VendorID) trips FROM taxi_rides_ny.yellow_taxi_data_non_partitoned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT COUNT(DISTINCT VendorID) trips 
FROM taxi_rides_ny.yellow_taxi_data_partitoned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';