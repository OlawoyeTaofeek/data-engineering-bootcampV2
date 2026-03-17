
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type
from `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup`
where payment_type is null



  
  
      
    ) dbt_internal_test