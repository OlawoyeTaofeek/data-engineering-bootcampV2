select * from (
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
)
SELECT * FROM green_data
) as __preview_sbq__ limit 1000