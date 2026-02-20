
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
where total_amount is null



  
  
      
    ) dbt_internal_test