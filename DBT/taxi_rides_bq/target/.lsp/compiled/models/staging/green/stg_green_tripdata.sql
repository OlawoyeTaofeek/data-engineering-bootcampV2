with tripdata as (
    SELECT *,
           row_number() OVER (PARTITION BY vendorid, lpep_pickup_datetime ORDER BY lpep_pickup_datetime) AS row_num
    FROM `datazoomcamp-486715`.`dbt_homework4`.`green_tripdata`
    WHERE vendorid IS NOT NULL
)
SELECT 
    to_hex(md5(cast(coalesce(cast(vendorid as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lpep_pickup_datetime as string), '_dbt_utils_surrogate_key_null_') as string))) as tripid,
    -- identifier
    CAST(vendorid AS INTEGER) AS vendor_id,
    
    
        safe_cast(ratecodeid as INTEGER)
    
 AS rate_code_id,
    CAST(pulocationid AS INTEGER) AS pickup_location_id,
    CAST(dolocationid AS INTEGER) AS dropoff_location_id,

    -- timestamp
    CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    
    
        safe_cast(trip_type as INTEGER)
    
 AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    
    
        safe_cast(payment_type as INTEGER)
    
 AS payment_type
FROM tripdata
WHERE row_num = 1