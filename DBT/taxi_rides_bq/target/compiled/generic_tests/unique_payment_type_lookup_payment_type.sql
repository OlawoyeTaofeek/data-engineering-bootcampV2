
    
    

with dbt_test__target as (

  select payment_type as unique_field
  from `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup`
  where payment_type is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


