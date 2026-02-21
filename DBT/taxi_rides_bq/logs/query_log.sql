-- created_at: 2026-02-21T17:02:05.036374700+00:00
-- finished_at: 2026-02-21T17:02:08.317814800+00:00
-- elapsed: 3.3s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: MiGosv2xgNZQz8YbVKabHvc0LHz
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select distinct schema_name from `datazoomcamp-486715`.INFORMATION_SCHEMA.SCHEMATA;
  ;
-- created_at: 2026-02-21T17:02:08.320127700+00:00
-- finished_at: 2026-02-21T17:02:09.567211700+00:00
-- elapsed: 1.2s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: uYiH2S1ca0Kl0JqQI2WvSOvnnX2
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "taxi_rides_bq", "target_name": "dev"} */
create schema if not exists `ny_taxi_rides`;
-- created_at: 2026-02-21T17:02:09.670772900+00:00
-- finished_at: 2026-02-21T17:02:12.423020800+00:00
-- elapsed: 2.8s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: iM0tZ8BoFSunAJXcAXvRRlQJOju
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-21T17:02:09.652838100+00:00
-- finished_at: 2026-02-21T17:02:12.471371100+00:00
-- elapsed: 2.8s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: 3hGmtfxlxQviDMjqzluuuOrTru2
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-21T17:02:09.796871500+00:00
-- finished_at: 2026-02-21T17:02:12.644442600+00:00
-- elapsed: 2.8s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.stg_green_tripdata
-- query_id: KVdFagH3N47OkH12dfrwiKeU5m7
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-21T17:02:09.808547600+00:00
-- finished_at: 2026-02-21T17:02:12.738349500+00:00
-- elapsed: 2.9s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.stg_yellow_tripdata
-- query_id: OuwwR7m7CEkWX4OkUm9Tz7wEnvg
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-21T17:02:12.425783700+00:00
-- finished_at: 2026-02-21T17:02:15.708220100+00:00
-- elapsed: 3.3s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: not available
-- desc: load_dataframe
;
-- created_at: 2026-02-21T17:02:12.476075900+00:00
-- finished_at: 2026-02-21T17:02:16.455463+00:00
-- elapsed: 4.0s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: not available
-- desc: load_dataframe
;
-- created_at: 2026-02-21T17:02:15.711474500+00:00
-- finished_at: 2026-02-21T17:02:17.989073400+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: wBWoKlgUPm4Hvi6xnDjAQ3GKH5A
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.payment_type_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    alter table `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup` set OPTIONS()
  ;
-- created_at: 2026-02-21T17:02:16.460962800+00:00
-- finished_at: 2026-02-21T17:02:18.802927300+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: ksTCV1vLhXZy0b9RkFGhjwVgcEO
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.zone_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    alter table `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup` set OPTIONS()
  ;
-- created_at: 2026-02-21T17:02:12.658327200+00:00
-- finished_at: 2026-02-21T17:02:20.439521800+00:00
-- elapsed: 7.8s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.stg_green_tripdata
-- query_id: 8d8yjoFI4PJhjwQnkn0MxBk8lfB
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.stg_green_tripdata", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`stg_green_tripdata`
      
    
    

    
    OPTIONS()
    as (
      with tripdata as (
    SELECT *,
           row_number() OVER (PARTITION BY vendorid, lpep_pickup_datetime ORDER BY lpep_pickup_datetime) AS row_num
    FROM `datazoomcamp-486715`.`dbt_homework4`.`green_tripdata`
    WHERE vendorid IS NOT NULL
)
SELECT 
    to_hex(md5(cast(coalesce(cast(vendorid as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(lpep_pickup_datetime as string), '_dbt_utils_surrogate_key_null_') as string))) as tripid,
    -- identifier
    CAST(vendorid AS INTEGER) AS vendor_id,
    
    
        safe_cast(ratecodeid as INTEGER)
    
 AS rate_code_id,
    CAST(pulocationid AS INTEGER) AS pickup_location_id,
    CAST(dolocationid AS INTEGER) AS dropoff_location_id,

    -- timestamp
    CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,
    
    
        safe_cast(trip_type as INTEGER)
    
 AS trip_type,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(ehail_fee AS NUMERIC) AS ehail_fee,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    
    
        safe_cast(payment_type as INTEGER)
    
 AS payment_type
FROM tripdata
WHERE row_num = 1
    );
  ;
-- created_at: 2026-02-21T17:02:18.012249900+00:00
-- finished_at: 2026-02-21T17:02:22.146029700+00:00
-- elapsed: 4.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_payment_type_lookup_payment_type.51dda45cb4
-- query_id: sjJFnvTDNtRBMEai7oT8Sp9DPxa
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_payment_type_lookup_payment_type.51dda45cb4", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select payment_type
from `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup`
where payment_type is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:18.017816200+00:00
-- finished_at: 2026-02-21T17:02:22.308907700+00:00
-- elapsed: 4.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_payment_type_lookup_payment_type.819f837245
-- query_id: cWMSh0J5xI1Nw7v2BDdGzHgFw2R
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.unique_payment_type_lookup_payment_type.819f837245", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:20.453978600+00:00
-- finished_at: 2026-02-21T17:02:23.222408400+00:00
-- elapsed: 2.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.accepted_values_stg_green_trip_8cb858912f43d9059b5dee071b0aff2f.8093128cd9
-- query_id: tWuUqbTHOQyiOwJrIfPdDRcWT4c
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.accepted_values_stg_green_trip_8cb858912f43d9059b5dee071b0aff2f.8093128cd9", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



    
    

with all_values as (

    select
        payment_type as value_field,
        count(*) as n_records

    from `datazoomcamp-486715`.`ny_taxi_rides`.`stg_green_tripdata`
    group by payment_type

)

select *
from all_values
where value_field not in (
    1,2,3,4,5,6
)



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:18.842421400+00:00
-- finished_at: 2026-02-21T17:02:24.195826400+00:00
-- elapsed: 5.4s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.dim_zones
-- query_id: IubWd38kzKnZaY2KbKC2w1WXIdh
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.dim_zones", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones`
      
    
    

    
    OPTIONS()
    as (
      SELECT 
    locationid as location_id,
    borough,
    zone,
    service_zone
FROM `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup`
    );
  ;
-- created_at: 2026-02-21T17:02:20.464244400+00:00
-- finished_at: 2026-02-21T17:02:24.490156400+00:00
-- elapsed: 4.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_green_tripdata_vendor_id.48b68c1493
-- query_id: D3emOVBJxv9dN8ChJaClUVzKn6q
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_stg_green_tripdata_vendor_id.48b68c1493", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendor_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`stg_green_tripdata`
where vendor_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:20.471967+00:00
-- finished_at: 2026-02-21T17:02:24.613348+00:00
-- elapsed: 4.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_green_tripdata_pickup_datetime.4c4eb3b86e
-- query_id: DPH52psYTIO6jGShQshFVv2f8YE
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_stg_green_tripdata_pickup_datetime.4c4eb3b86e", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_datetime
from `datazoomcamp-486715`.`ny_taxi_rides`.`stg_green_tripdata`
where pickup_datetime is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:24.211216200+00:00
-- finished_at: 2026-02-21T17:02:26.558962800+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_dim_zones_location_id.cbf42e7b79
-- query_id: cu6pUkYcV7k9MEfWhbQHiTeFlzy
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_dim_zones_location_id.cbf42e7b79", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones`
where location_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:24.211216200+00:00
-- finished_at: 2026-02-21T17:02:26.976531700+00:00
-- elapsed: 2.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_dim_zones_location_id.5aa0ad7f27
-- query_id: KyQHnYgQI0UTGd7zwzDv0ekywcj
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.unique_dim_zones_location_id.5aa0ad7f27", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select location_id as unique_field
  from `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones`
  where location_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:12.751730900+00:00
-- finished_at: 2026-02-21T17:02:27.955650400+00:00
-- elapsed: 15.2s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.stg_yellow_tripdata
-- query_id: BUnJxRdbSwCtK6MnxhpeZtJG2YW
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.stg_yellow_tripdata", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`stg_yellow_tripdata`
      
    
    

    
    OPTIONS()
    as (
      with tripdata as (
    SELECT *,
           row_number() OVER (PARTITION BY vendorid, tpep_pickup_datetime ORDER BY tpep_pickup_datetime) AS row_num
    FROM `datazoomcamp-486715`.`dbt_homework4`.`yellow_tripdata`
    WHERE vendorid IS NOT NULL
)
SELECT 
    -- identifier
    to_hex(md5(cast(coalesce(cast(vendorid as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(tpep_pickup_datetime as string), '_dbt_utils_surrogate_key_null_') as string))) as tripid,
    CAST(vendorid AS INTEGER) AS vendor_id,
    
    
        safe_cast(ratecodeid as INTEGER)
    
 AS rate_code_id,
    CAST(pulocationid AS INTEGER) AS pickup_location_id,
    CAST(dolocationid AS INTEGER) AS dropoff_location_id,

    -- timestamp
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    CAST(store_and_fwd_flag AS STRING) AS store_and_fwd_flag,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS NUMERIC) AS trip_distance,

    -- payment info
    CAST(fare_amount AS NUMERIC) AS fare_amount,
    CAST(extra AS NUMERIC) AS extra,
    CAST(mta_tax AS NUMERIC) AS mta_tax,
    CAST(tip_amount AS NUMERIC) AS tip_amount,
    CAST(tolls_amount AS NUMERIC) AS tolls_amount,
    CAST(improvement_surcharge AS NUMERIC) AS improvement_surcharge,
    CAST(total_amount AS NUMERIC) AS total_amount,
    
    
        safe_cast(payment_type as INTEGER)
    
 AS payment_type
FROM tripdata
WHERE row_num = 1

  
    );
  ;
-- created_at: 2026-02-21T17:02:27.968550700+00:00
-- finished_at: 2026-02-21T17:02:30.484604100+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_yellow_tripdata_pickup_datetime.140d66ac44
-- query_id: ma7hOg4QmeW2wBAFfxZn5CMS6i6
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_stg_yellow_tripdata_pickup_datetime.140d66ac44", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_datetime
from `datazoomcamp-486715`.`ny_taxi_rides`.`stg_yellow_tripdata`
where pickup_datetime is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:27.969509800+00:00
-- finished_at: 2026-02-21T17:02:30.484607100+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_yellow_tripdata_vendor_id.85d1b1a751
-- query_id: ys5IUUcVnA4WaxwDfdYbSwXX1BK
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_stg_yellow_tripdata_vendor_id.85d1b1a751", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendor_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`stg_yellow_tripdata`
where vendor_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:30.508511400+00:00
-- finished_at: 2026-02-21T17:02:38.876676300+00:00
-- elapsed: 8.4s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.taxi_rides
-- query_id: viI6L1axVk5hEbjQrIUzbzKq6x9
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.taxi_rides", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`taxi_rides`
      
    
    

    
    OPTIONS()
    as (
      

with green_data as (
    SELECT 
        tripid,
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type, 
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'Green' as service_type
    FROM `datazoomcamp-486715`.`ny_taxi_rides`.`stg_green_tripdata`
),
yellow_data as (
    SELECT 
        tripid,
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        CAST(1 as INTEGER) as trip_type, -- Yellow taxi doesn't have trip_type, so we set it to 1 (Street-hail) since most yellow taxi rides are street-hail
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        CAST(0 as NUMERIC) as ehail_fee, -- Yellow taxi doesn't have ehail_fee, so we set it to 0
        improvement_surcharge,
        total_amount,
        payment_type,
        'Yellow' as service_type
    FROM `datazoomcamp-486715`.`ny_taxi_rides`.`stg_yellow_tripdata`
)
SELECT *
FROM green_data 
UNION ALL
SELECT *
FROM yellow_data
    );
  ;
-- created_at: 2026-02-21T17:02:38.895626800+00:00
-- finished_at: 2026-02-21T17:02:46.091051+00:00
-- elapsed: 7.2s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.int_trips
-- query_id: lPwmwlXQQAH37ljUEoL1e379tVO
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.int_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
      
    
    

    
    OPTIONS()
    as (
      

with unioned as (
    SELECT 
       *
    FROM `datazoomcamp-486715`.`ny_taxi_rides`.`taxi_rides`
),
payment_types as (
    SELECT *
    from `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup`
),
clean_and_enriched as (
    SELECT 
 -- Generate unique trip identifier (surrogate key pattern)
        to_hex(md5(cast(coalesce(cast(u.vendor_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(u.pickup_datetime as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(u.pickup_location_id as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(u.service_type as string), '_dbt_utils_surrogate_key_null_') as string))) as trip_id,

        -- Identifiers
        u.vendor_id,
        u.service_type,
        u.rate_code_id,

        -- Location IDs
        u.pickup_location_id,
        u.dropoff_location_id,

        -- Timestamps
        u.pickup_datetime,
        u.dropoff_datetime,

        -- Trip details
        u.store_and_fwd_flag,
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,

        -- Enrich with payment type description
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pty.description, 'Unknown') as payment_type_description
    FROM unioned as u
    LEFT JOIN payment_types AS pty
    ON coalesce(u.payment_type, 0) = pty.payment_type
)
SELECT 
   *
FROM clean_and_enriched
    );
  ;
-- created_at: 2026-02-21T17:02:46.102142900+00:00
-- finished_at: 2026-02-21T17:02:48.532354800+00:00
-- elapsed: 2.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_service_type.f38e150f53
-- query_id: cGXAGNorokOLDf7g9mqOtxIhYYk
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_int_trips_service_type.f38e150f53", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select service_type
from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
where service_type is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:46.103270300+00:00
-- finished_at: 2026-02-21T17:02:48.538227+00:00
-- elapsed: 2.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_vendor_id.c9ac94714c
-- query_id: rHXTEcD3Hky8GWuEdhqJKUww3dg
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_int_trips_vendor_id.c9ac94714c", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendor_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
where vendor_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:46.109666800+00:00
-- finished_at: 2026-02-21T17:02:49.356535900+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.accepted_values_int_trips_service_type__Green__Yellow.0c65c5e307
-- query_id: exJi42cAIBT6DaoU3ie3qiJ4HOX
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.accepted_values_int_trips_service_type__Green__Yellow.0c65c5e307", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        service_type as value_field,
        count(*) as n_records

    from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
    group by service_type

)

select *
from all_values
where value_field not in (
    'Green','Yellow'
)



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:46.118753900+00:00
-- finished_at: 2026-02-21T17:02:49.492045700+00:00
-- elapsed: 3.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_total_amount.ec933efa9b
-- query_id: QQY71fSStmrnmuD2Hb2JndTuj4i
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_int_trips_total_amount.ec933efa9b", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount
from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
where total_amount is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:46.123077900+00:00
-- finished_at: 2026-02-21T17:02:49.508407700+00:00
-- elapsed: 3.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_pickup_datetime.e4fa5f3e09
-- query_id: YgsgGWhclAPW1SVvyksj4T2FktE
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_int_trips_pickup_datetime.e4fa5f3e09", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_datetime
from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
where pickup_datetime is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:46.113398100+00:00
-- finished_at: 2026-02-21T17:02:49.652255800+00:00
-- elapsed: 3.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_trip_id.9edd5fee61
-- query_id: L7ZCuNdSWbFFb44Gc4B68fh88rz
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_int_trips_trip_id.9edd5fee61", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
where trip_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:46.104652500+00:00
-- finished_at: 2026-02-21T17:02:50.686824900+00:00
-- elapsed: 4.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_int_trips_trip_id.37d69d814f
-- query_id: oXbbmw10kJXVgIzrEaCRRQgSiaP
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.unique_int_trips_trip_id.37d69d814f", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select trip_id as unique_field
  from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips`
  where trip_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:02:50.760877300+00:00
-- finished_at: 2026-02-21T17:02:52.654618400+00:00
-- elapsed: 1.9s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: LKfbgH3U4b5GA5pC7hU85rLSefT
-- desc: get_column_schema_from_query adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */
select * from (
        -- Fact table containing all taxi trips enriched with zone information
-- This is a classic star schema design: fact table (trips) joined to dimension table (zones)
-- Materialized incrementally to handle large datasets efficiently

select
    -- Trip identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    
    

    datetime_diff(
        cast(trips.dropoff_datetime as datetime),
        cast(trips.pickup_datetime as datetime),
        minute
    )

  
 as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips` as trips
-- LEFT JOIN preserves all trips even if zone information is missing or unknown
left join `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones` as pz
    on trips.pickup_location_id = pz.location_id
left join `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones` as dz
    on trips.dropoff_location_id = dz.location_id
    ) as __dbt_sbq
    where false and current_timestamp() = current_timestamp()
    limit 0
;
-- created_at: 2026-02-21T17:02:52.712785500+00:00
-- finished_at: 2026-02-21T17:02:54.763381300+00:00
-- elapsed: 2.1s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: Em7SNkV28gqr5shbEr1LcjaU76O
-- desc: get_column_schema_from_query adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */
select * from (
        select
    
      
    cast(null as string)
 as trip_id, 
      
    cast(null as integer)
 as vendor_id, 
      
    cast(null as string)
 as service_type, 
      
    cast(null as integer)
 as rate_code_id, 
      
    cast(null as integer)
 as pickup_location_id, 
      
    cast(null as string)
 as pickup_borough, 
      
    cast(null as string)
 as pickup_zone, 
      
    cast(null as integer)
 as dropoff_location_id, 
      
    cast(null as string)
 as dropoff_borough, 
      
    cast(null as string)
 as dropoff_zone, 
      
    cast(null as timestamp)
 as pickup_datetime, 
      
    cast(null as timestamp)
 as dropoff_datetime, 
      
    cast(null as string)
 as store_and_fwd_flag, 
      
    cast(null as integer)
 as passenger_count, 
      
    cast(null as numeric)
 as trip_distance, 
      
    cast(null as integer)
 as trip_type, 
      
    cast(null as bigint)
 as trip_duration_minutes, 
      
    cast(null as numeric)
 as fare_amount, 
      
    cast(null as numeric)
 as extra, 
      
    cast(null as numeric)
 as mta_tax, 
      
    cast(null as numeric)
 as tip_amount, 
      
    cast(null as numeric)
 as tolls_amount, 
      
    cast(null as numeric)
 as ehail_fee, 
      
    cast(null as numeric)
 as improvement_surcharge, 
      
    cast(null as numeric)
 as total_amount, 
      
    cast(null as integer)
 as payment_type, 
      
    cast(null as string)
 as payment_type_description
    ) as __dbt_sbq
    where false and current_timestamp() = current_timestamp()
    limit 0
;
-- created_at: 2026-02-21T17:02:54.822990400+00:00
-- finished_at: 2026-02-21T17:03:01.842430700+00:00
-- elapsed: 7.0s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: 2lloxx3osiXL9plPm4KxysP1qvY
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
        
  (
    trip_id string,
    vendor_id integer,
    service_type string,
    rate_code_id integer,
    pickup_location_id integer,
    pickup_borough string,
    pickup_zone string,
    dropoff_location_id integer,
    dropoff_borough string,
    dropoff_zone string,
    pickup_datetime timestamp,
    dropoff_datetime timestamp,
    store_and_fwd_flag string,
    passenger_count integer,
    trip_distance numeric,
    trip_type integer,
    trip_duration_minutes bigint,
    fare_amount numeric,
    extra numeric,
    mta_tax numeric,
    tip_amount numeric,
    tolls_amount numeric,
    ehail_fee numeric,
    improvement_surcharge numeric,
    total_amount numeric,
    payment_type integer,
    payment_type_description string
    
    )

      
    
    

    
    OPTIONS()
    as (
      
    select trip_id, vendor_id, service_type, rate_code_id, pickup_location_id, pickup_borough, pickup_zone, dropoff_location_id, dropoff_borough, dropoff_zone, pickup_datetime, dropoff_datetime, store_and_fwd_flag, passenger_count, trip_distance, trip_type, trip_duration_minutes, fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee, improvement_surcharge, total_amount, payment_type, payment_type_description
    from (
        -- Fact table containing all taxi trips enriched with zone information
-- This is a classic star schema design: fact table (trips) joined to dimension table (zones)
-- Materialized incrementally to handle large datasets efficiently

select
    -- Trip identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough as pickup_borough,
    pz.zone as pickup_zone,
    trips.dropoff_location_id,
    dz.borough as dropoff_borough,
    dz.zone as dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    
    

    datetime_diff(
        cast(trips.dropoff_datetime as datetime),
        cast(trips.pickup_datetime as datetime),
        minute
    )

  
 as trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

from `datazoomcamp-486715`.`ny_taxi_rides`.`int_trips` as trips
-- LEFT JOIN preserves all trips even if zone information is missing or unknown
left join `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones` as pz
    on trips.pickup_location_id = pz.location_id
left join `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones` as dz
    on trips.dropoff_location_id = dz.location_id
    ) as model_subq
    );
  ;
-- created_at: 2026-02-21T17:03:01.862293300+00:00
-- finished_at: 2026-02-21T17:03:05.842167900+00:00
-- elapsed: 4.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_fct_trips_trip_id.9b74cb6aab
-- query_id: qmwgN3i0gJHry4uKjlPTq158ySm
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.unique_fct_trips_trip_id.9b74cb6aab", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select trip_id as unique_field
  from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
  where trip_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.950607900+00:00
-- finished_at: 2026-02-21T17:03:06.523063100+00:00
-- elapsed: 4.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_total_amount.0e8ec5477d
-- query_id: DGmnbkA0l86sQF5QBTacxbzPASt
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_trips_total_amount.0e8ec5477d", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_amount
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
where total_amount is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.962899600+00:00
-- finished_at: 2026-02-21T17:03:06.545527100+00:00
-- elapsed: 4.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_pickup_datetime.3c78164d83
-- query_id: bVD2prHqLHpbGRI3Nu588cqxaMW
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_trips_pickup_datetime.3c78164d83", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_datetime
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
where pickup_datetime is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.913190400+00:00
-- finished_at: 2026-02-21T17:03:07.145226700+00:00
-- elapsed: 5.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_trip_id.df65bf8c1d
-- query_id: ajrnyCUwM0JjgBRyvVuKqtIeGYo
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_trips_trip_id.df65bf8c1d", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trip_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
where trip_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.901175500+00:00
-- finished_at: 2026-02-21T17:03:07.298346600+00:00
-- elapsed: 5.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.accepted_values_fct_trips_service_type__Green__Yellow.f40032e124
-- query_id: pTis0iOsR7kcYeD8eSiAB2a3o7o
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.accepted_values_fct_trips_service_type__Green__Yellow.f40032e124", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

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



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.940299200+00:00
-- finished_at: 2026-02-21T17:03:07.635422200+00:00
-- elapsed: 5.7s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_service_type.f7d55d2d8f
-- query_id: zczECx22NAZ8CsEv8oVMCBeeRFH
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_trips_service_type.f7d55d2d8f", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select service_type
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
where service_type is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.930990900+00:00
-- finished_at: 2026-02-21T17:03:07.682248700+00:00
-- elapsed: 5.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.relationships_fct_trips_d47e97bfe07500b11a10d61b874aa24c.55becb7a81
-- query_id: AP9oT9QKUa4nTu0so6gyzKP1X11
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.relationships_fct_trips_d47e97bfe07500b11a10d61b874aa24c.55becb7a81", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select pickup_location_id as from_field
    from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
    where pickup_location_id is not null
),

parent as (
    select location_id as to_field
    from `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.886648900+00:00
-- finished_at: 2026-02-21T17:03:09.912334100+00:00
-- elapsed: 8.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_vendor_id.b9b2f380bb
-- query_id: y4fUbTjuaEG5BQVSmP2ncdQ9ggt
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_trips_vendor_id.b9b2f380bb", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendor_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
where vendor_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:01.922235800+00:00
-- finished_at: 2026-02-21T17:03:10.357271100+00:00
-- elapsed: 8.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.relationships_fct_trips_ad3060576ca78bd47560ec9e21a96e3f.177a7c1703
-- query_id: P6dpN2G9pzjP4J63gJacExr58t6
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.relationships_fct_trips_ad3060576ca78bd47560ec9e21a96e3f.177a7c1703", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select dropoff_location_id as from_field
    from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
    where dropoff_location_id is not null
),

parent as (
    select location_id as to_field
    from `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:10.376707400+00:00
-- finished_at: 2026-02-21T17:03:13.803334+00:00
-- elapsed: 3.4s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.dim_vendors
-- query_id: ax4KBOw8Q5qiT1P513C9Yl3lX8Y
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.dim_vendors", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`dim_vendors`
      
    
    

    
    OPTIONS()
    as (
      -- Dimension table for taxi technology vendors
-- Small static dimension defining vendor codes and their company names

with trips as (
    select * from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
),

vendors as (
    select distinct
        vendor_id,
        



case vendor_id
    
    when 1 then 'Creative Mobile Technologies'
    
    when 2 then 'VeriFone Inc.'
    
    when 4 then 'Unknown/Other'
    
end

 as vendor_name
    from trips
)

select * from vendors
    );
  ;
-- created_at: 2026-02-21T17:03:10.375121900+00:00
-- finished_at: 2026-02-21T17:03:14.948415800+00:00
-- elapsed: 4.6s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_monthly_zone_revenue
-- query_id: 89sn8Ok07JkPeJ14v4hFOvwsDop
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_monthly_zone_revenue", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
      
    
    

    
    OPTIONS()
    as (
      -- Data mart for monthly revenue analysis by pickup zone and service type
-- This aggregation is optimized for business reporting and dashboards
-- Enables analysis of revenue trends across different zones and taxi types



select
    -- Grouping dimensions
    coalesce(pickup_zone, 'Unknown Zone') as pickup_zone,
    cast(date_trunc(pickup_datetime, month) as date)
     as revenue_month,
    service_type,

    -- Revenue breakdown (summed by zone, month, and service type)
    sum(fare_amount) as revenue_monthly_fare,
    sum(extra) as revenue_monthly_extra,
    sum(mta_tax) as revenue_monthly_mta_tax,
    sum(tip_amount) as revenue_monthly_tip_amount,
    sum(tolls_amount) as revenue_monthly_tolls_amount,
    sum(ehail_fee) as revenue_monthly_ehail_fee,
    sum(improvement_surcharge) as revenue_monthly_improvement_surcharge,
    sum(total_amount) as revenue_monthly_total_amount,

    -- Additional metrics for operational analysis
    count(trip_id) as total_monthly_trips,
    avg(passenger_count) as avg_monthly_passenger_count,
    avg(trip_distance) as avg_monthly_trip_distance

from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`
group by pickup_zone, revenue_month, service_type
    );
  ;
-- created_at: 2026-02-21T17:03:13.825613500+00:00
-- finished_at: 2026-02-21T17:03:16.229064200+00:00
-- elapsed: 2.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_dim_vendors_vendor_id.8a24aa2141
-- query_id: FyvReYKsgNcmKAjvdIJJ8NjNDbY
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.unique_dim_vendors_vendor_id.8a24aa2141", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with dbt_test__target as (

  select vendor_id as unique_field
  from `datazoomcamp-486715`.`ny_taxi_rides`.`dim_vendors`
  where vendor_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:13.825043500+00:00
-- finished_at: 2026-02-21T17:03:16.371897700+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_dim_vendors_vendor_id.a23aa97580
-- query_id: WjT6PTwRbPoODrJw1G2AgJmhWLj
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_dim_vendors_vendor_id.a23aa97580", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select vendor_id
from `datazoomcamp-486715`.`ny_taxi_rides`.`dim_vendors`
where vendor_id is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:14.963058400+00:00
-- finished_at: 2026-02-21T17:03:17.236738700+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_total_monthly_trips.c1d9a37413
-- query_id: BKsY4quk5u8ZYl5cuw7b7qnGG3W
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_total_monthly_trips.c1d9a37413", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_monthly_trips
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where total_monthly_trips is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:14.959641300+00:00
-- finished_at: 2026-02-21T17:03:17.247091900+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_service_type.7967f8c6c7
-- query_id: jboCP3UlI0KjY0r4STPwM2tdqPy
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_service_type.7967f8c6c7", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select service_type
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where service_type is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:14.964053+00:00
-- finished_at: 2026-02-21T17:03:17.264857500+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.dbt_utils_unique_combination_o_961556abbbe77853a8075a2259256569.6e62b32871
-- query_id: MJi8Ggj4G6wRwL6mAuwRkJFfCa8
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.dbt_utils_unique_combination_o_961556abbbe77853a8075a2259256569.6e62b32871", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        pickup_zone, revenue_month, service_type
    from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
    group by pickup_zone, revenue_month, service_type
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:14.960245700+00:00
-- finished_at: 2026-02-21T17:03:17.272007300+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_revenue_monthly_total_amount.7dd10042fe
-- query_id: zbIxklxgm03GVNmUijqW8AmaGMD
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_revenue_monthly_total_amount.7dd10042fe", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select revenue_monthly_total_amount
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where revenue_monthly_total_amount is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:14.962360800+00:00
-- finished_at: 2026-02-21T17:03:17.298213800+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_revenue_month.50fb349100
-- query_id: wa9ZRO6hegttqWZbRukH5ozIETS
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_revenue_month.50fb349100", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select revenue_month
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where revenue_month is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:14.964443900+00:00
-- finished_at: 2026-02-21T17:03:17.347454700+00:00
-- elapsed: 2.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_pickup_zone.e3683be709
-- query_id: v6KgpSgPEnYrOwZ7lYCpPahJ2wm
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_pickup_zone.e3683be709", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select pickup_zone
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where pickup_zone is null



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:14.964321800+00:00
-- finished_at: 2026-02-21T17:03:17.347454800+00:00
-- elapsed: 2.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.accepted_values_fct_monthly_zo_dbc49f52f2319a3f5827fb3bd27223a5.ad84d0930a
-- query_id: EI10MNfH90NFoB5zxONuMf6OwgF
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "test.taxi_rides_bq.accepted_values_fct_monthly_zo_dbc49f52f2319a3f5827fb3bd27223a5.ad84d0930a", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        service_type as value_field,
        count(*) as n_records

    from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
    group by service_type

)

select *
from all_values
where value_field not in (
    'Green','Yellow'
)



  
  
      
    ) dbt_internal_test;
-- created_at: 2026-02-21T17:03:17.365310900+00:00
-- finished_at: 2026-02-21T17:03:20.680360300+00:00
-- elapsed: 3.3s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment3
-- query_id: sqm7F1FqXFcuNsIPJagKwBgDRzR
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.assignment3", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`assignment3`
      
    
    

    
    OPTIONS()
    as (
      select
    count(*) as total_records
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
    );
  ;
-- created_at: 2026-02-21T17:03:17.368501100+00:00
-- finished_at: 2026-02-21T17:03:20.863346200+00:00
-- elapsed: 3.5s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment
-- query_id: 3ZcDZcXwuGMVcexnW4hsuli2SlM
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.assignment", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

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
  ;
-- created_at: 2026-02-21T17:03:17.368501200+00:00
-- finished_at: 2026-02-21T17:03:20.896538600+00:00
-- elapsed: 3.5s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment2
-- query_id: 6bL86hGLnBYQmGO7humC8qQE2YI
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.assignment2", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`assignment2`
      
    
    

    
    OPTIONS()
    as (
      select
    sum(total_monthly_trips) as total_trips_oct_2019
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where service_type = 'Green'
  and revenue_month BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'   
    );
  ;
