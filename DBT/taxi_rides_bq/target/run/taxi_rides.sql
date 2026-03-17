
  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`taxi_rides`
      
    
    

    
    OPTIONS()
    as (
      

with green_data as (
    SELECT 
        tripid,
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type, 
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'Green' as service_type
    FROM `datazoomcamp-486715`.`ny_taxi_rides`.`stg_green_tripdata`
),
yellow_data as (
    SELECT 
        tripid,
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        CAST(1 as INTEGER) as trip_type, -- Yellow taxi doesn't have trip_type, so we set it to 1 (Street-hail) since most yellow taxi rides are street-hail
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        CAST(0 as NUMERIC) as ehail_fee, -- Yellow taxi doesn't have ehail_fee, so we set it to 0
        improvement_surcharge,
        total_amount,
        payment_type,
        'Yellow' as service_type
    FROM `datazoomcamp-486715`.`ny_taxi_rides`.`stg_yellow_tripdata`
)
SELECT *
FROM green_data 
UNION ALL
SELECT *
FROM yellow_data
    );
  