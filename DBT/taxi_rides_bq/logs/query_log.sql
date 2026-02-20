-- created_at: 2026-02-20T12:05:26.808358900+00:00
-- finished_at: 2026-02-20T12:05:30.068285900+00:00
-- elapsed: 3.3s
-- outcome: success
-- dialect: bigquery
-- node_id: not available
-- query_id: iJBGjRdJSL0QCeEHiokVzRBuXhw
-- desc: execute adapter call
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "taxi_rides_bq", "target_name": "dev"} */

    select distinct schema_name from `datazoomcamp-486715`.INFORMATION_SCHEMA.SCHEMATA;
  ;
-- created_at: 2026-02-20T12:05:30.111686400+00:00
-- finished_at: 2026-02-20T12:05:33.264390600+00:00
-- elapsed: 3.2s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment3
-- query_id: L5eRm9D5BFZdJphfgOccRF1XVcr
-- desc: get_relation > list_relations call
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_type
FROM 
    `datazoomcamp-486715`.`ny_taxi_rides`.INFORMATION_SCHEMA.TABLES;
-- created_at: 2026-02-20T12:05:33.273383300+00:00
-- finished_at: 2026-02-20T12:05:35.763521400+00:00
-- elapsed: 2.5s
-- outcome: success
-- dialect: bigquery
-- node_id: model.taxi_rides_bq.assignment3
-- query_id: pbVn8FdWmlNobj2hJey2Gi1AEvj
-- desc: execute adapter call
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.taxi_rides_bq.assignment3", "profile_name": "taxi_rides_bq", "target_name": "dev"} */


  create or replace view `datazoomcamp-486715`.`ny_taxi_rides`.`assignment3`
  OPTIONS()
  as select
    count(*) as total_records
from `datazoomcamp-486715`.`ny_taxi_rides`.`fct_monthly_zone_revenue`;

;
