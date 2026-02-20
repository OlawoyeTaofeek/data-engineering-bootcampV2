select * from (
with tripdata as (
    SELECT *,
           row_number() OVER (PARTITION BY vendorid, lpep_pickup_datetime ORDER BY lpep_pickup_datetime) AS row_num
    FROM `datazoomcamp-486715`.`dbt_homework4`.`green_tripdata`
    WHERE vendorid IS NOT NULL
)
SELECT * FROM tripdata
) as __preview_sbq__ limit 1000