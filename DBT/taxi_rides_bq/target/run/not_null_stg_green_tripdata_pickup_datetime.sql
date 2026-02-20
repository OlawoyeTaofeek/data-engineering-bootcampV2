
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_datetime
from `datazoomcamp-486715`.`ny_taxi_rides`.`stg_green_tripdata`
where pickup_datetime is null



  
  
      
    ) dbt_internal_test