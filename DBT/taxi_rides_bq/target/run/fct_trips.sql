
  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
        
  (
    trip_id string,
    vendor_id integer,
    service_type string,
    rate_code_id integer,
    pickup_location_id integer,
    pickup_borough string,
    pickup_zone string,
    dropoff_location_id integer,
    dropoff_borough string,
    dropoff_zone string,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    store_and_fwd_flag string,
    passenger_count integer,
    trip_distance numeric,
    trip_type integer,
    trip_duration_minutes bigint,
    fare_amount numeric,
    extra numeric,
    mta_tax numeric,
    tip_amount numeric,
    tolls_amount numeric,
    ehail_fee numeric,
    improvement_surcharge numeric,
    total_amount numeric,
    payment_type integer,
    payment_type_description string
    
    )

      
    
    

    
    OPTIONS()
    as (
      
    select trip_id, vendor_id, service_type, rate_code_id, pickup_location_id, pickup_borough, pickup_zone, dropoff_location_id, dropoff_borough, dropoff_zone, pickup_datetime, dropoff_datetime, store_and_fwd_flag, passenger_count, trip_distance, trip_type, trip_duration_minutes, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, payment_type_description
    from (
        -- Fact table containing all taxi trips enriched with zone information
-- This is a classic star schema design: fact table (trips) joined to dimension table (zones)
-- Materialized incrementally to handle large datasets efficiently

select
    -- Trip identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    
    

    datetime_diff(
        cast(trips.dropoff_datetime as datetime),
        cast(trips.pickup_datetime as datetime),
        minute
    )

  
 as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips` as trips
-- LEFT JOIN preserves all trips even if zone information is missing or unknown
left join `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones` as pz
    on trips.pickup_location_id = pz.location_id
left join `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones` as dz
    on trips.dropoff_location_id = dz.location_id
    ) as model_subq
    );
  