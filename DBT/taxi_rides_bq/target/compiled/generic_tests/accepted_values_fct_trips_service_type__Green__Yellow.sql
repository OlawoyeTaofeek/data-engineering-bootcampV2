
    
    

with all_values as (

    select
        service_type as value_field,
        count(*) as n_records

    from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
    group by service_type

)

select *
from all_values
where value_field not in (
    'Green','Yellow'
)


