
  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`assignment`
      
    
    

    
    OPTIONS()
    as (
      select
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_revenue_2020
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where service_type = 'Green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by total_revenue_2020 desc
limit 1
    );
  