/* @bruin

# Peak day analysis: identifies the day with highest trips and highest total fare

name: reports.peak_days_analysis

type: duckdb.sql

# Depends on the staging trips data
depends:
  - staging.trips

# Create+replace strategy
materialization:
  type: table
  strategy: create+replace

# Define schema
columns:
  - name: metric_type
    type: string
    description: Type of metric (highest_trips or highest_fare)
  - name: peak_date
    type: date
    description: Date with the peak metric value
  - name: metric_value
    type: double
    description: The numeric value of the peak metric
  - name: taxi_type
    type: string
    description: Taxi type indicator
  - name: total_trips
    type: bigint
    description: Total trips on the peak date
  - name: total_fare
    type: double
    description: Total fare revenue on the peak date

@bruin */

-- Aggregate daily totals from staging trips
WITH daily_trip_totals AS (
  SELECT
    DATE(t.pickup_datetime) AS trip_date,
    COUNT(*) AS total_trips,
    SUM(t.total_amount) AS total_fare_amount
  FROM staging.trips t
  GROUP BY DATE(t.pickup_datetime)
),
ranked_by_trips AS (
  SELECT
    'highest_trips' AS metric_type,
    trip_date AS peak_date,
    CAST(total_trips AS DOUBLE) AS metric_value,
    'combined' AS taxi_type,
    total_trips,
    total_fare_amount AS total_fare,
    ROW_NUMBER() OVER (ORDER BY total_trips DESC) AS trip_rank
  FROM daily_trip_totals
),
ranked_by_fare AS (
  SELECT
    'highest_fare' AS metric_type,
    trip_date AS peak_date,
    total_fare_amount AS metric_value,
    'combined' AS taxi_type,
    total_trips,
    total_fare_amount AS total_fare,
    ROW_NUMBER() OVER (ORDER BY total_fare_amount DESC) AS fare_rank
  FROM daily_trip_totals
)
SELECT metric_type, peak_date, metric_value, taxi_type, total_trips, total_fare
FROM ranked_by_trips
WHERE trip_rank = 1

UNION ALL

SELECT metric_type, peak_date, metric_value, taxi_type, total_trips, total_fare
FROM ranked_by_fare
WHERE fare_rank = 1
