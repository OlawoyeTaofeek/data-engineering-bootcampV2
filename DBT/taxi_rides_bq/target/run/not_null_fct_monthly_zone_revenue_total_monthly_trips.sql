
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_monthly_trips
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where total_monthly_trips is null



  
  
      
    ) dbt_internal_test