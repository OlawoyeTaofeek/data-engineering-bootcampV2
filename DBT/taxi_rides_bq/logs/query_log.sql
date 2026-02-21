-- created_at: 2026-02-20T23:42:47.007523700+00:00
-- finished_at: 2026-02-20T23:42:52.194127300+00:00
-- elapsed: 5.2s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: OW3DUUQxYOv7gV9P2i9gsTbBjx4
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select distinct schema_name from `datazoomcamp-486715`.INFORMATION_SCHEMA.SCHEMATA;
  ;
-- created_at: 2026-02-20T23:42:52.232304100+00:00
-- finished_at: 2026-02-20T23:42:55.854146100+00:00
-- elapsed: 3.6s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: TyVxIryOgrURgz8Qf91qXjr36AD
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-20T23:42:52.233688900+00:00
-- finished_at: 2026-02-20T23:42:57.525542200+00:00
-- elapsed: 5.3s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: ZSWVMCcDZL2wxbAZXLx0O0BoZWp
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-20T23:42:55.862349+00:00
-- finished_at: 2026-02-20T23:42:57.840456700+00:00
-- elapsed: 2.0s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: YLmc5plvjVVXt6E3k7IwK4vyaPk
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.payment_type_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    drop table if exists `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup`
;
-- created_at: 2026-02-20T23:42:57.530744600+00:00
-- finished_at: 2026-02-20T23:42:59.344899800+00:00
-- elapsed: 1.8s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: kOPQZCvYH4ztcMx8AO9Q79PFUly
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.zone_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    drop table if exists `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup`
;
-- created_at: 2026-02-20T23:42:57.842001600+00:00
-- finished_at: 2026-02-20T23:43:02.540290500+00:00
-- elapsed: 4.7s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: not available
-- desc: load_dataframe
;
-- created_at: 2026-02-20T23:42:59.346321100+00:00
-- finished_at: 2026-02-20T23:43:02.701655900+00:00
-- elapsed: 3.4s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: not available
-- desc: load_dataframe
;
-- created_at: 2026-02-20T23:43:02.543576600+00:00
-- finished_at: 2026-02-20T23:43:05.981724400+00:00
-- elapsed: 3.4s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.payment_type_lookup
-- query_id: RStubqh3pwpIlsFClLAz54YngRW
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.payment_type_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    alter table `datazoomcamp-486715`.`ny_taxi_rides`.`payment_type_lookup` set OPTIONS()
  ;
-- created_at: 2026-02-20T23:43:02.703292300+00:00
-- finished_at: 2026-02-20T23:43:06.874623900+00:00
-- elapsed: 4.2s
-- outcome: success
-- dialect: bigquery
-- node_id: seed.taxi_rides_bq.zone_lookup
-- query_id: PDKLI3qyT5DWi0Mt9G7nEUSiClG
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "seed.taxi_rides_bq.zone_lookup", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    alter table `datazoomcamp-486715`.`ny_taxi_rides`.`zone_lookup` set OPTIONS()
  ;
