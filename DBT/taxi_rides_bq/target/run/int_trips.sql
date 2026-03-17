
  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
      
    
    

    
    OPTIONS()
    as (
      

with unioned as (
    SELECT 
       *
    FROM `datazoomcamp-486715`.`ny_taxi_rides`.`taxi_rides`
),
payment_types as (
    SELECT *
    from `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup`
),
clean_and_enriched as (
    SELECT 
 -- Generate unique trip identifier (surrogate key pattern)
        to_hex(md5(cast(coalesce(cast(u.vendor_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(u.pickup_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(u.pickup_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(u.service_type as string), '_dbt_utils_surrogate_key_null_') as string))) as trip_id,

        -- Identifiers
        u.vendor_id,
        u.service_type,
        u.rate_code_id,

        -- Location IDs
        u.pickup_location_id,
        u.dropoff_location_id,

        -- Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- Trip details
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,

        -- Enrich with payment type description
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pty.description, 'Unknown') as payment_type_description
    FROM unioned as u
    LEFT JOIN payment_types AS pty
    ON coalesce(u.payment_type, 0) = pty.payment_type
)
SELECT 
   *
FROM clean_and_enriched
    );
  