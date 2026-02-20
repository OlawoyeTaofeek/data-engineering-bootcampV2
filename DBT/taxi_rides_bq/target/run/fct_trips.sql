-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips` as DBT_INTERNAL_DEST
        using (
        select
        * from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.trip_id = DBT_INTERNAL_DEST.trip_id))

    
    when matched then update set
        `trip_id` = DBT_INTERNAL_SOURCE.`trip_id`,`vendor_id` = DBT_INTERNAL_SOURCE.`vendor_id`,`service_type` = DBT_INTERNAL_SOURCE.`service_type`,`rate_code_id` = DBT_INTERNAL_SOURCE.`rate_code_id`,`pickup_location_id` = DBT_INTERNAL_SOURCE.`pickup_location_id`,`pickup_borough` = DBT_INTERNAL_SOURCE.`pickup_borough`,`pickup_zone` = DBT_INTERNAL_SOURCE.`pickup_zone`,`dropoff_location_id` = DBT_INTERNAL_SOURCE.`dropoff_location_id`,`dropoff_borough` = DBT_INTERNAL_SOURCE.`dropoff_borough`,`dropoff_zone` = DBT_INTERNAL_SOURCE.`dropoff_zone`,`pickup_datetime` = DBT_INTERNAL_SOURCE.`pickup_datetime`,`dropoff_datetime` = DBT_INTERNAL_SOURCE.`dropoff_datetime`,`store_and_fwd_flag` = DBT_INTERNAL_SOURCE.`store_and_fwd_flag`,`passenger_count` = DBT_INTERNAL_SOURCE.`passenger_count`,`trip_distance` = DBT_INTERNAL_SOURCE.`trip_distance`,`trip_type` = DBT_INTERNAL_SOURCE.`trip_type`,`trip_duration_minutes` = DBT_INTERNAL_SOURCE.`trip_duration_minutes`,`fare_amount` = DBT_INTERNAL_SOURCE.`fare_amount`,`extra` = DBT_INTERNAL_SOURCE.`extra`,`mta_tax` = DBT_INTERNAL_SOURCE.`mta_tax`,`tip_amount` = DBT_INTERNAL_SOURCE.`tip_amount`,`tolls_amount` = DBT_INTERNAL_SOURCE.`tolls_amount`,`ehail_fee` = DBT_INTERNAL_SOURCE.`ehail_fee`,`improvement_surcharge` = DBT_INTERNAL_SOURCE.`improvement_surcharge`,`total_amount` = DBT_INTERNAL_SOURCE.`total_amount`,`payment_type` = DBT_INTERNAL_SOURCE.`payment_type`,`payment_type_description` = DBT_INTERNAL_SOURCE.`payment_type_description`
    

    when not matched then insert
        (`trip_id`, `vendor_id`, `service_type`, `rate_code_id`, `pickup_location_id`, `pickup_borough`, `pickup_zone`, `dropoff_location_id`, `dropoff_borough`, `dropoff_zone`, `pickup_datetime`, `dropoff_datetime`, `store_and_fwd_flag`, `passenger_count`, `trip_distance`, `trip_type`, `trip_duration_minutes`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `ehail_fee`, `improvement_surcharge`, `total_amount`, `payment_type`, `payment_type_description`)
    values
        (`trip_id`, `vendor_id`, `service_type`, `rate_code_id`, `pickup_location_id`, `pickup_borough`, `pickup_zone`, `dropoff_location_id`, `dropoff_borough`, `dropoff_zone`, `pickup_datetime`, `dropoff_datetime`, `store_and_fwd_flag`, `passenger_count`, `trip_distance`, `trip_type`, `trip_duration_minutes`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `ehail_fee`, `improvement_surcharge`, `total_amount`, `payment_type`, `payment_type_description`)


    