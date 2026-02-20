
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
where trip_id is null



  
  
      
    ) dbt_internal_test