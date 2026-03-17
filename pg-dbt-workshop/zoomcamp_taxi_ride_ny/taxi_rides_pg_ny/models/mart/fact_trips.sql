{{
    config(
        materialized="table"
    )
}}

with trips_combined as (
    select * from {{ ref('tripdata') }}
),

dim_zones_filtered as (
    select * 
    from {{ ref('dim_zones') }}
    where "Borough" != 'unknown'  -- filter out unknown zones
)

select
    tc.tripid,
    tc.vendor_id,
    tc.rate_code_id,
    tc.pickup_location_id,
    tc.dropoff_location_id,
    tc.pickup_datetime,
    tc.dropoff_datetime,
    {{ get_trip_duration_minutes('tc.pickup_datetime', 'tc.dropoff_datetime') }} as trip_duration_minutes,
    tc.store_and_fwd_flag,
    tc.passenger_count,
    tc.trip_distance,
    tc.trip_type,
    tc.fare_amount,
    tc.extra,
    tc.mta_tax,
    tc.tip_amount,
    tc.tolls_amount,
    tc.ehail_fee,
    tc.improvement_surcharge,
    tc.total_amount,
    tc.payment_type_name,
    tc.service_type,
    pz."Zone" as pickup_zone,
    dz."Zone" as dropoff_zone
from trips_combined tc
inner join dim_zones_filtered pz 
    on tc.pickup_location_id = pz.location_id
inner join dim_zones_filtered dz 
    on tc.dropoff_location_id = dz.location_id
