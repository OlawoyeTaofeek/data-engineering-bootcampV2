
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select revenue_month
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where revenue_month is null



  
  
      
    ) dbt_internal_test