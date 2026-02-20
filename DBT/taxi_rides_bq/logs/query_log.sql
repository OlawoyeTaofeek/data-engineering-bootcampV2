-- created_at: 2026-02-20T17:58:20.231080900+00:00
-- finished_at: 2026-02-20T17:58:23.936707300+00:00
-- elapsed: 3.7s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: irjgbQwM5O9vC1LurvJHmxtcWaK
-- desc: list_relations_in_parallel
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-20T17:58:25.584046700+00:00
-- finished_at: 2026-02-20T17:58:29.176787700+00:00
-- elapsed: 3.6s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: k25CiQdwLPTMd8S8Tdwm3PfCcFB
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select distinct schema_name from `datazoomcamp-486715`.INFORMATION_SCHEMA.SCHEMATA;
  ;
-- created_at: 2026-02-20T17:58:29.241359700+00:00
-- finished_at: 2026-02-20T17:58:31.195454500+00:00
-- elapsed: 2.0s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: NF6i93aVgCEpL9FOzoI0nyWepxH
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.payment_type_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    drop table if exists `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup`
;
-- created_at: 2026-02-20T17:58:29.250865100+00:00
-- finished_at: 2026-02-20T17:58:31.201749+00:00
-- elapsed: 2.0s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: FYAGQnvWmYlrliw6rkpkrL15Onw
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.zone_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    drop table if exists `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup`
;
-- created_at: 2026-02-20T17:58:31.199420400+00:00
-- finished_at: 2026-02-20T17:58:33.846420400+00:00
-- elapsed: 2.6s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: not available
-- desc: load_dataframe
;
-- created_at: 2026-02-20T17:58:31.206021300+00:00
-- finished_at: 2026-02-20T17:58:35.622360700+00:00
-- elapsed: 4.4s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: not available
-- desc: load_dataframe
;
-- created_at: 2026-02-20T17:58:33.849040300+00:00
-- finished_at: 2026-02-20T17:58:36.167979100+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: nfQeiT6LegBKTTLTlnI9TTMJR9n
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.payment_type_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    alter table `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup` set OPTIONS()
  ;
-- created_at: 2026-02-20T17:58:30.423741700+00:00
-- finished_at: 2026-02-20T17:58:37.754129300+00:00
-- elapsed: 7.3s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.stg_yellow_tripdata
-- query_id: s5gztPKv1KCHyGfpthDnGYkkgVN
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


   limit 100
  
    );
  ;
-- created_at: 2026-02-20T17:58:30.443300300+00:00
-- finished_at: 2026-02-20T17:58:38.020134900+00:00
-- elapsed: 7.6s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.stg_green_tripdata
-- query_id: 5vKu7PD0KA2ZQHb5KuKVR153szd
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
-- created_at: 2026-02-20T17:58:35.626482+00:00
-- finished_at: 2026-02-20T17:58:38.058172500+00:00
-- elapsed: 2.4s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: SuQVX7kFNKtcN2QwFExK0Xe1sU4
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.zone_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    alter table `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup` set OPTIONS()
  ;
-- created_at: 2026-02-20T17:58:36.189239100+00:00
-- finished_at: 2026-02-20T17:58:39.342447400+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_payment_type_lookup_payment_type.51dda45cb4
-- query_id: TAzuiNE5pW0YvIqogdUt7zZhJtK
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
-- created_at: 2026-02-20T17:58:36.184975300+00:00
-- finished_at: 2026-02-20T17:58:39.609376500+00:00
-- elapsed: 3.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_payment_type_lookup_payment_type.819f837245
-- query_id: ru91ssfWkiLdxl742cQvIIg7qfO
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
-- created_at: 2026-02-20T17:58:38.104743400+00:00
-- finished_at: 2026-02-20T17:58:40.826740+00:00
-- elapsed: 2.7s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.dim_zones
-- query_id: USAngFAodMk63flqu9tHG8vhO05
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.dim_zones", "profile_name": "taxi_rides_bq", "target_name": "dev"} */


  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`dim_zones`
  OPTIONS()
  as SELECT 
    locationid as location_id,
    borough,
    zone,
    service_zone
FROM `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup`;

;
-- created_at: 2026-02-20T17:58:37.776471+00:00
-- finished_at: 2026-02-20T17:58:40.888403400+00:00
-- elapsed: 3.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_yellow_tripdata_vendor_id.85d1b1a751
-- query_id: DZplFYSHzPNh1m7ItDw2drIhKnn
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
-- created_at: 2026-02-20T17:58:38.058912700+00:00
-- finished_at: 2026-02-20T17:58:41.174448900+00:00
-- elapsed: 3.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_green_tripdata_vendor_id.48b68c1493
-- query_id: qdGN2PJNTtBaMuq7IsaiTG8XMRP
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
-- created_at: 2026-02-20T17:58:38.043553800+00:00
-- finished_at: 2026-02-20T17:58:41.293143500+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_green_tripdata_pickup_datetime.4c4eb3b86e
-- query_id: F0cSuobKN17cXUWY1ct22eg61hA
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
-- created_at: 2026-02-20T17:58:37.784626100+00:00
-- finished_at: 2026-02-20T17:58:41.462603300+00:00
-- elapsed: 3.7s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_stg_yellow_tripdata_pickup_datetime.140d66ac44
-- query_id: k6THDJyAXMuvvYCqe5HqcRo9gOk
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
-- created_at: 2026-02-20T17:58:40.838302700+00:00
-- finished_at: 2026-02-20T17:58:43.369855300+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_dim_zones_location_id.5aa0ad7f27
-- query_id: 2g5N0WadjwHKXjwHz1mzT87B16P
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
-- created_at: 2026-02-20T17:58:40.838302100+00:00
-- finished_at: 2026-02-20T17:58:43.485316+00:00
-- elapsed: 2.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_dim_zones_location_id.cbf42e7b79
-- query_id: RE27yR1wkZ3nGnb16caHXI6pMTm
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
-- created_at: 2026-02-20T17:58:41.819276300+00:00
-- finished_at: 2026-02-20T17:58:48.266664200+00:00
-- elapsed: 6.4s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.taxi_rides
-- query_id: LgsTIYvTJ7edaCIAiP5mZzGJqEl
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
-- created_at: 2026-02-20T17:58:48.588322200+00:00
-- finished_at: 2026-02-20T17:58:55.967379800+00:00
-- elapsed: 7.4s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.int_trips
-- query_id: qJFNupwR1gAGMx2fglOfmiXH3Ca
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
-- created_at: 2026-02-20T17:58:56.053240600+00:00
-- finished_at: 2026-02-20T17:58:58.492413200+00:00
-- elapsed: 2.4s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_service_type.f38e150f53
-- query_id: VvWKisqJsxJpXvMYIYWgPqGbaIY
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
-- created_at: 2026-02-20T17:58:56.036545100+00:00
-- finished_at: 2026-02-20T17:58:58.666721600+00:00
-- elapsed: 2.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.accepted_values_int_trips_service_type__Green__Yellow.0c65c5e307
-- query_id: s5jSbGRTsqGAJjdjP3kOTyRCSFW
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
-- created_at: 2026-02-20T17:58:56.174433500+00:00
-- finished_at: 2026-02-20T17:58:59.171946400+00:00
-- elapsed: 3.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_trip_id.9edd5fee61
-- query_id: 4uxUm7yqml3GzFyKz5l4CA63vi6
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
-- created_at: 2026-02-20T17:58:56.193731900+00:00
-- finished_at: 2026-02-20T17:58:59.244694100+00:00
-- elapsed: 3.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_vendor_id.c9ac94714c
-- query_id: reowGdEL1cT5lxZQVWH7TkGNgLu
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
-- created_at: 2026-02-20T17:58:56.136601700+00:00
-- finished_at: 2026-02-20T17:58:59.310745200+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_pickup_datetime.e4fa5f3e09
-- query_id: zK1glDL9HbmQ3IiU3WPPvQ55Hq3
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
-- created_at: 2026-02-20T17:58:56.185281800+00:00
-- finished_at: 2026-02-20T17:58:59.368699100+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_int_trips_total_amount.ec933efa9b
-- query_id: 8M445jVs460F8S1XUT42ukRHGkl
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
-- created_at: 2026-02-20T17:58:56.069489500+00:00
-- finished_at: 2026-02-20T17:59:00.103584900+00:00
-- elapsed: 4.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_int_trips_trip_id.37d69d814f
-- query_id: TXTTFYs10QXvUUlqZk66qT5wdG8
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
-- created_at: 2026-02-20T17:59:00.122027400+00:00
-- finished_at: 2026-02-20T17:59:02.066637600+00:00
-- elapsed: 1.9s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: oMxkqOcsPUKXP3PgMhyGQNofg81
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


  -- Only process new trips based on pickup datetime
  where trips.pickup_datetime > (select max(pickup_datetime) from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`)

    ) as __dbt_sbq
    where false and current_timestamp() = current_timestamp()
    limit 0
;
-- created_at: 2026-02-20T17:59:02.127337700+00:00
-- finished_at: 2026-02-20T17:59:04.010054100+00:00
-- elapsed: 1.9s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: F1J3Ly1dH9LTlOaJs7hixkM5TcI
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
-- created_at: 2026-02-20T17:59:04.093048100+00:00
-- finished_at: 2026-02-20T17:59:07.428283800+00:00
-- elapsed: 3.3s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: tinVjfVeG9IqJmlcRawdwtCotqb
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

  
    

    create or replace table `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips__dbt_tmp`
        
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

      
    
    

    
    OPTIONS(
      expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 12 hour)
    )
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


  -- Only process new trips based on pickup datetime
  where trips.pickup_datetime > (select max(pickup_datetime) from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips`)

    ) as model_subq
    );
  ;
-- created_at: 2026-02-20T17:59:07.432866600+00:00
-- finished_at: 2026-02-20T17:59:09.699530300+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: FVCWDjjvRZksUZtX4zhy8vWCPd5
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    SELECT
                column_name,
                data_type
             FROM `datazoomcamp-486715.ny_taxi_rides.INFORMATION_SCHEMA.COLUMNS`
             WHERE table_schema = 'ny_taxi_rides' and table_name = 'fct_trips__dbt_tmp'
             ORDER BY ordinal_position

  ;
-- created_at: 2026-02-20T17:59:09.708204700+00:00
-- finished_at: 2026-02-20T17:59:12.044128400+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: i1bYjeB6Kf1Cs1ZXk3Hmt7DZK1C
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    SELECT
                column_name,
                data_type
             FROM `datazoomcamp-486715.ny_taxi_rides.INFORMATION_SCHEMA.COLUMNS`
             WHERE table_schema = 'ny_taxi_rides' and table_name = 'fct_trips'
             ORDER BY ordinal_position

  ;
-- created_at: 2026-02-20T17:59:12.130574100+00:00
-- finished_at: 2026-02-20T17:59:16.774910600+00:00
-- elapsed: 4.6s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: YCOulEaO5pgVdpwKDK2edkShsr2
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */
-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips` as DBT_INTERNAL_DEST
        using (
        select
        * from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.trip_id = DBT_INTERNAL_DEST.trip_id))

    
    when matched then update set
        `trip_id` = DBT_INTERNAL_SOURCE.`trip_id`,`vendor_id` = DBT_INTERNAL_SOURCE.`vendor_id`,`service_type` = DBT_INTERNAL_SOURCE.`service_type`,`rate_code_id` = DBT_INTERNAL_SOURCE.`rate_code_id`,`pickup_location_id` = DBT_INTERNAL_SOURCE.`pickup_location_id`,`pickup_borough` = DBT_INTERNAL_SOURCE.`pickup_borough`,`pickup_zone` = DBT_INTERNAL_SOURCE.`pickup_zone`,`dropoff_location_id` = DBT_INTERNAL_SOURCE.`dropoff_location_id`,`dropoff_borough` = DBT_INTERNAL_SOURCE.`dropoff_borough`,`dropoff_zone` = DBT_INTERNAL_SOURCE.`dropoff_zone`,`pickup_datetime` = DBT_INTERNAL_SOURCE.`pickup_datetime`,`dropoff_datetime` = DBT_INTERNAL_SOURCE.`dropoff_datetime`,`store_and_fwd_flag` = DBT_INTERNAL_SOURCE.`store_and_fwd_flag`,`passenger_count` = DBT_INTERNAL_SOURCE.`passenger_count`,`trip_distance` = DBT_INTERNAL_SOURCE.`trip_distance`,`trip_type` = DBT_INTERNAL_SOURCE.`trip_type`,`trip_duration_minutes` = DBT_INTERNAL_SOURCE.`trip_duration_minutes`,`fare_amount` = DBT_INTERNAL_SOURCE.`fare_amount`,`extra` = DBT_INTERNAL_SOURCE.`extra`,`mta_tax` = DBT_INTERNAL_SOURCE.`mta_tax`,`tip_amount` = DBT_INTERNAL_SOURCE.`tip_amount`,`tolls_amount` = DBT_INTERNAL_SOURCE.`tolls_amount`,`ehail_fee` = DBT_INTERNAL_SOURCE.`ehail_fee`,`improvement_surcharge` = DBT_INTERNAL_SOURCE.`improvement_surcharge`,`total_amount` = DBT_INTERNAL_SOURCE.`total_amount`,`payment_type` = DBT_INTERNAL_SOURCE.`payment_type`,`payment_type_description` = DBT_INTERNAL_SOURCE.`payment_type_description`
    

    when not matched then insert
        (`trip_id`, `vendor_id`, `service_type`, `rate_code_id`, `pickup_location_id`, `pickup_borough`, `pickup_zone`, `dropoff_location_id`, `dropoff_borough`, `dropoff_zone`, `pickup_datetime`, `dropoff_datetime`, `store_and_fwd_flag`, `passenger_count`, `trip_distance`, `trip_type`, `trip_duration_minutes`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `ehail_fee`, `improvement_surcharge`, `total_amount`, `payment_type`, `payment_type_description`)
    values
        (`trip_id`, `vendor_id`, `service_type`, `rate_code_id`, `pickup_location_id`, `pickup_borough`, `pickup_zone`, `dropoff_location_id`, `dropoff_borough`, `dropoff_zone`, `pickup_datetime`, `dropoff_datetime`, `store_and_fwd_flag`, `passenger_count`, `trip_distance`, `trip_type`, `trip_duration_minutes`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `ehail_fee`, `improvement_surcharge`, `total_amount`, `payment_type`, `payment_type_description`)


    ;
-- created_at: 2026-02-20T17:59:16.794563800+00:00
-- finished_at: 2026-02-20T17:59:18.092419700+00:00
-- elapsed: 1.3s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_trips
-- query_id: IudRcldZcrH1kqpgslsKbDf9vuh
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_trips", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    drop table if exists `datazoomcamp-486715`.`ny_taxi_rides`.`fct_trips__dbt_tmp`
;
-- created_at: 2026-02-20T17:59:18.105662100+00:00
-- finished_at: 2026-02-20T17:59:20.447228400+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_pickup_datetime.3c78164d83
-- query_id: Fyl7fpwRDVzPPbCIN0Tn66EvtsQ
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
-- created_at: 2026-02-20T17:59:18.172951900+00:00
-- finished_at: 2026-02-20T17:59:21.088807200+00:00
-- elapsed: 2.9s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_total_amount.0e8ec5477d
-- query_id: VbdxvEyWjIlPEiJxCHDVK7IuBPN
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
-- created_at: 2026-02-20T17:59:18.143324+00:00
-- finished_at: 2026-02-20T17:59:21.122596400+00:00
-- elapsed: 3.0s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.accepted_values_fct_trips_service_type__Green__Yellow.f40032e124
-- query_id: LJ3VkT2uNy4fhenOhwkLJQS0JhA
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
-- created_at: 2026-02-20T17:59:18.127041300+00:00
-- finished_at: 2026-02-20T17:59:21.199123200+00:00
-- elapsed: 3.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_trip_id.df65bf8c1d
-- query_id: wMY2UiUMLSPIe34ZkjCweHuqBnc
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
-- created_at: 2026-02-20T17:59:18.150250500+00:00
-- finished_at: 2026-02-20T17:59:21.285115+00:00
-- elapsed: 3.1s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_vendor_id.b9b2f380bb
-- query_id: FnlCiQAKjYs64bVbMMbkuH6pOSg
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
-- created_at: 2026-02-20T17:59:18.157616500+00:00
-- finished_at: 2026-02-20T17:59:21.364051900+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_trips_service_type.f7d55d2d8f
-- query_id: sTDM8trqa9rXlAKEzcws2WN33XN
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
-- created_at: 2026-02-20T17:59:18.180127800+00:00
-- finished_at: 2026-02-20T17:59:21.513199300+00:00
-- elapsed: 3.3s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.relationships_fct_trips_ad3060576ca78bd47560ec9e21a96e3f.177a7c1703
-- query_id: CUg9RuXKZhI7FORB7LPTekJikaG
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
-- created_at: 2026-02-20T17:59:18.135679600+00:00
-- finished_at: 2026-02-20T17:59:21.667202900+00:00
-- elapsed: 3.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.relationships_fct_trips_d47e97bfe07500b11a10d61b874aa24c.55becb7a81
-- query_id: c6bKMhpiAAAXb7ZsARffjapTBQ6
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
-- created_at: 2026-02-20T17:59:18.165069100+00:00
-- finished_at: 2026-02-20T17:59:22.977106900+00:00
-- elapsed: 4.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_fct_trips_trip_id.9b74cb6aab
-- query_id: dxO45znBhkkNnuOAq1wnJotdPuD
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
-- created_at: 2026-02-20T17:59:22.992609500+00:00
-- finished_at: 2026-02-20T17:59:24.856220400+00:00
-- elapsed: 1.9s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.dim_vendors
-- query_id: JOIBUpkQ0griDcl4yOMXvZ7ls0s
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.dim_vendors", "profile_name": "taxi_rides_bq", "target_name": "dev"} */


  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`dim_vendors`
  OPTIONS()
  as -- Dimension table for taxi technology vendors
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

select * from vendors;

;
-- created_at: 2026-02-20T17:59:22.992829+00:00
-- finished_at: 2026-02-20T17:59:25.339188400+00:00
-- elapsed: 2.3s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.fct_monthly_zone_revenue
-- query_id: 8HobLGLLtLsLsJmivGf5rI1SrZG
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.fct_monthly_zone_revenue", "profile_name": "taxi_rides_bq", "target_name": "dev"} */


  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
  OPTIONS()
  as -- Data mart for monthly revenue analysis by pickup zone and service type
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
group by pickup_zone, revenue_month, service_type;

;
-- created_at: 2026-02-20T17:59:24.868306500+00:00
-- finished_at: 2026-02-20T17:59:27.377722500+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_dim_vendors_vendor_id.a23aa97580
-- query_id: i7TwKr6abkqq24FOPApz6FGafeZ
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
-- created_at: 2026-02-20T17:59:24.868331500+00:00
-- finished_at: 2026-02-20T17:59:27.646634700+00:00
-- elapsed: 2.8s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.unique_dim_vendors_vendor_id.8a24aa2141
-- query_id: VWnVITileHoRRCRWYectgHLEVlz
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
-- created_at: 2026-02-20T17:59:25.361943100+00:00
-- finished_at: 2026-02-20T17:59:27.856933500+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_service_type.7967f8c6c7
-- query_id: H2Y8kTLJdJfeZnCxnZyhV2kS7Vp
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
-- created_at: 2026-02-20T17:59:25.360071300+00:00
-- finished_at: 2026-02-20T17:59:27.898665700+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.accepted_values_fct_monthly_zo_dbc49f52f2319a3f5827fb3bd27223a5.ad84d0930a
-- query_id: xbvGsvWlNvets1raWjv0dfIAWOI
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
-- created_at: 2026-02-20T17:59:25.366914500+00:00
-- finished_at: 2026-02-20T17:59:27.923754200+00:00
-- elapsed: 2.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_total_monthly_trips.c1d9a37413
-- query_id: nsdbH8v8u92D7LQGiTie6gsDRb7
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
-- created_at: 2026-02-20T17:59:25.360637900+00:00
-- finished_at: 2026-02-20T17:59:27.980370+00:00
-- elapsed: 2.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_pickup_zone.e3683be709
-- query_id: 93TZicxHgFOH8aZhNb5OYQBCBhc
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
-- created_at: 2026-02-20T17:59:25.367051300+00:00
-- finished_at: 2026-02-20T17:59:27.995450100+00:00
-- elapsed: 2.6s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_revenue_monthly_total_amount.7dd10042fe
-- query_id: Sy87v48GBleUZQZf1KZzJX2kZoa
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
-- created_at: 2026-02-20T17:59:25.361851400+00:00
-- finished_at: 2026-02-20T17:59:28.285657500+00:00
-- elapsed: 2.9s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.dbt_utils_unique_combination_o_961556abbbe77853a8075a2259256569.6e62b32871
-- query_id: lg2CaBehz4MltTjWj0SQICBDsbx
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
-- created_at: 2026-02-20T17:59:25.364928900+00:00
-- finished_at: 2026-02-20T17:59:28.543806500+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: test.taxi_rides_bq.not_null_fct_monthly_zone_revenue_revenue_month.50fb349100
-- query_id: n3wT99jWOrqBRUxq2tRYmHUTlPW
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
-- created_at: 2026-02-20T17:59:28.569118200+00:00
-- finished_at: 2026-02-20T17:59:30.458514+00:00
-- elapsed: 1.9s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment3
-- query_id: 9IgJodLOK3MJGvde2i4ASsItVU2
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.assignment3", "profile_name": "taxi_rides_bq", "target_name": "dev"} */


  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`assignment3`
  OPTIONS()
  as select
    count(*) as total_records
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`;

;
-- created_at: 2026-02-20T17:59:28.568861+00:00
-- finished_at: 2026-02-20T17:59:30.660998600+00:00
-- elapsed: 2.1s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment
-- query_id: GORzNOLHJlUlMGM72MkJVajLeV7
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.assignment", "profile_name": "taxi_rides_bq", "target_name": "dev"} */


  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`assignment`
  OPTIONS()
  as select
    pickup_zone,
    sum(revenue_monthly_total_amount) as total_revenue_2020
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where service_type = 'Green'
  and extract(year from revenue_month) = 2020
group by pickup_zone
order by total_revenue_2020 desc
limit 1;

;
-- created_at: 2026-02-20T17:59:28.568756200+00:00
-- finished_at: 2026-02-20T17:59:30.743825300+00:00
-- elapsed: 2.2s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment2
-- query_id: bjHgNIytlFRJ9uBsUt5z35bn9wv
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.assignment2", "profile_name": "taxi_rides_bq", "target_name": "dev"} */


  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`assignment2`
  OPTIONS()
  as select
    sum(total_monthly_trips) as total_trips_oct_2019
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`
where service_type = 'Green'
  and revenue_month BETWEEN DATE '2019-10-01' AND DATE '2019-10-31'   ;

;
