
  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`assignment3`
      
    
    

    
    OPTIONS()
    as (
      select
    count(*) as total_records
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
    );
  