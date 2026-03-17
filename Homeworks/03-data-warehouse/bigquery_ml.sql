-- Machine Learninh model in BigQuery Cloud
CREATE OR REPLACE TABLE `bq_machine_learning.yellow_tripdata_ml` (
  `passenger_count` INTEGER,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DULocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
) AS (
  SELECT 
    passenger_count, 
    trip_distance, 
    CAST(PULocationID as STRING),
    CAST(DOLocationID as STRING),
    CAST(payment_type as STRING),
    fare_amount, tolls_amount, 
    tip_amount
  FROM `datazoomcamp-486715.dw_homework3.yellow_tripdata_partitioned`
  WHERE fare_amount != 0
);

-- CREATE MODEL WITH DEFAUT SETTING
CREATE OR REPLACE MODEL `datazoomcamp-486715.bq_machine_learning.tip_model`
OPTIONS (
  model_type = 'linear_reg',
  input_label_cols = ['tip_amount'],
  data_split_method = 'AUTO_SPLIT'
) AS
SELECT *
FROM `datazoomcamp-486715.bq_machine_learning.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;

-- CHECK FEATURES 
SELECT 
  *
FROM ML.FEATURE_INFO(MODEL `datazoomcamp-486715.bq_machine_learning.tip_model`); 

-- EVALUATE OUR MODEL
SELECT *
FROM 
  ML.EVALUATE(MODEL `datazoomcamp-486715.bq_machine_learning.tip_model`, 
  (
    SELECT 
      * 
    FROM `datazoomcamp-486715.bq_machine_learning.yellow_tripdata_ml`
    WHERE 
      tip_amount IS not NULL
  ));

-- PREDICT MODEL
SELECT *
FROM ML.PREDICT(MODEL `datazoomcamp-486715.bq_machine_learning.tip_model`, (
  SELECT
    *
  FROM `datazoomcamp-486715.bq_machine_learning.yellow_tripdata_ml`
  WHERE tip_amount IS NOT NULL
))

-- PREDICT MODEL AND EXPLAIN 
SELECT *
FROM ML.EXPLAIN_PREDICT(MODEL `datazoomcamp-486715.bq_machine_learning.tip_model`, (
  SELECT
    *
  FROM `datazoomcamp-486715.bq_machine_learning.yellow_tripdata_ml`
    WHERE tip_amount IS NOT NULL
), STRUCT(3 AS top_k_features));

-- HYPER PARAM TUNNING
CREATE OR REPLACE MODEL `taxi-rides-ny.nytaxi.tip_hyperparam_model`
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT',
num_trials=5,
max_parallel_trials=2,
l1_reg=hparam_range(0, 20),
l2_reg=hparam_candidates([0, 0.1, 1, 10])) AS
SELECT
*
FROM
`taxi-rides-ny.nytaxi.yellow_tripdata_ml`
WHERE
tip_amount IS NOT NULL;














