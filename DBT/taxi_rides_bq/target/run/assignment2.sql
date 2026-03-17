
  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`assignment2`
      
    
    

    
    OPTIONS()
    as (
      select
    sum(total_monthly_trips) as total_trips_oct_2019
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where service_type = 'Green'
  and revenue_month BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'   
    );
  