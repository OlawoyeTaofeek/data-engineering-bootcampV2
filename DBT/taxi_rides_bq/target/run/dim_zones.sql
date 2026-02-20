

  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones`
  OPTIONS()
  as SELECT 
    locationid as location_id,
    borough,
    zone,
    service_zone
FROM `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup`;

