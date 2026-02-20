-- created_at: 2026-02-20T11:53:25.711115500+00:00
-- finished_at: 2026-02-20T11:53:29.531422500+00:00
-- elapsed: 3.8s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: TkfYQ7rtWilD7xRma5Mseo10gen
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select distinct schema_name from `datazoomcamp-486715`.INFORMATION_SCHEMA.SCHEMATA;
  ;
-- created_at: 2026-02-20T11:53:29.580361800+00:00
-- finished_at: 2026-02-20T11:53:33.371625600+00:00
-- elapsed: 3.8s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment2
-- query_id: fAUbPoMVcTSlsfSnDJPcYMd9UjL
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-20T11:53:33.379752200+00:00
-- finished_at: 2026-02-20T11:53:36.373933700+00:00
-- elapsed: 3.0s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment2
-- query_id: E0PfVkgiTdRpckswsq0qCUV5D85
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
