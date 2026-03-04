/* @bruin

# Reports layer: summarizes trip data for dashboards and analytics
#
# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

name: reports.trips_report

type: duckdb.sql

# Depends on the clean, deduplicated staging data
depends:
  - staging.trips

# Create+replace strategy: drops and recreates table on each run
# Suitable for report aggregations built from staging data
materialization:
  type: table
  strategy: create+replace

# Define aggregated report schema with quality checks
columns:
  - name: report_date
    type: date
    description: Date of the report (aggregation key)
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow, green, etc.)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Payment method name
    primary_key: true
    checks:
      - name: not_null
  - name: trip_count
    type: bigint
    description: Total number of trips
    checks:
      - name: not_null
      - name: positive
  - name: total_revenue
    type: double
    description: Sum of all fare amounts (total_amount)
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_fare
    type: double
    description: Average fare amount per trip
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_distance
    type: double
    description: Average trip distance in miles
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_passenger_count
    type: double
    description: Average passengers per trip
    checks:
      - name: not_null
      - name: non_negative
  - name: avg_tip_amount
    type: double
    description: Average tip per trip
    checks:
      - name: not_null
      - name: non_negative
  - name: min_trip_distance
    type: double
    description: Minimum trip distance in the period
    checks:
      - name: non_negative
  - name: max_trip_distance
    type: double
    description: Maximum trip distance in the period
    checks:
      - name: non_negative
  - name: min_total_amount
    type: double
    description: Minimum fare in the period
    checks:
      - name: non_negative
  - name: max_total_amount
    type: double
    description: Maximum fare in the period
    checks:
      - name: non_negative

# Custom quality checks for aggregated data
custom_checks:
  - name: positive_trip_counts
    description: All trip counts must be greater than zero
    query: |
      SELECT COUNT(*) FROM reports.trips_report
      WHERE trip_count <= 0
    value: 0
  - name: revenue_matches_fare_calculation
    description: Total revenue should approximately equal sum of average fares * trip count
    query: |
      SELECT COUNT(*) FROM reports.trips_report
      WHERE ABS(total_revenue - (avg_fare * trip_count)) > 1.0
    value: 0
  - name: valid_taxi_types
    description: (Optional) Validate taxi_type values are expected
    query: |
      SELECT COUNT(*) FROM reports.trips_report
      WHERE taxi_type NOT IN ('yellow', 'green', 'fhv', 'fhvhv')
    value: 0

@bruin */

-- Aggregate trips by date, taxi type, and payment method
-- Provides summary metrics for analytics and dashboards
SELECT
  DATE(t.pickup_datetime) AS report_date,
  t.taxi_type,
  t.payment_type_name,

  -- count metrics
  COUNT(*) AS trip_count,
  SUM(COALESCE(passenger_count, 0)) as total_passengers,

  -- Distance metrics 
  sum(COALESCE(trip_distance, 0)) as total_distance,

  -- Revenue metrics
  SUM(t.total_amount) AS total_revenue,
  AVG(t.fare_amount) AS avg_fare,
  SUM(COALESCE(fare_amount, 0)) as total_fare_amount,
  SUM(COALESCE(tip_amount, 0)) as total_tip_amount,

  AVG(t.trip_distance) AS avg_distance,
  AVG(t.passenger_count) AS avg_passenger_count,
  AVG(t.tip_amount) AS avg_tip_amount,
  MIN(t.trip_distance) AS min_trip_distance,
  MAX(t.trip_distance) AS max_trip_distance,
  MIN(t.total_amount) AS min_total_amount,
  MAX(t.total_amount) AS max_total_amount
FROM staging.trips t
GROUP BY
  DATE(t.pickup_datetime),
  t.taxi_type,
  t.payment_type_name
