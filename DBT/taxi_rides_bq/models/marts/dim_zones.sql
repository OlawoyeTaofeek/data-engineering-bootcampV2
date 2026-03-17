SELECT 
    locationid as location_id,
    borough,
    zone,
    service_zone
FROM {{ ref("zone_lookup") }}