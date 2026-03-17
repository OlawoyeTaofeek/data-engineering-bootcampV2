{{
    config(
        materialized='table'
    )
}}

select
    "LocationID" as location_id,
    "Borough",
    "Zone",
    replace("service_zone", 'Boro', 'Green') as service_zone
from {{ ref('taxi_zone_lookup') }}
