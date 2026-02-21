-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `datazoomcamp-486715.dw_homework3.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2026_bq_warehouse/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*)
FROM `datazoomcamp-486715.dw_homework3.external_yellow_tripdata`;


SELECT COUNT(DISTINCT PULocationID)
FROM `datazoomcamp-486715.dw_homework3.external_yellow_tripdata`;

SELECT COUNT(*)
FROM `datazoomcamp-486715.dw_homework3.external_yellow_tripdata`
WHERE fare_amount = 0;


CREATE OR REPLACE TABLE `datazoomcamp-486715.dw_homework3.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
AS
SELECT *
FROM `datazoomcamp-486715.dw_homework3.external_yellow_tripdata`;


SELECT DISTINCT VendorID
FROM `datazoomcamp-486715.dw_homework3.yellow_tripdata_partitioned`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01'
AND '2024-03-15 23:59:59';

SELECT DISTINCT VendorID
FROM `datazoomcamp-486715.dw_homework3.external_yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01'
AND '2024-03-15 23:59:59';


SELECT COUNT(*)
FROM `datazoomcamp-486715.dw_homework3.external_yellow_tripdata`;




