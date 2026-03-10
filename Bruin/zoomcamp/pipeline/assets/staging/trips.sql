/* @bruin

# Staging layer: cleans, deduplicates, and enriches raw trip data
#
# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

name: staging.trips

type: duckdb.sql

# Declare dependencies for lineage and downstream processing
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# Create+replace strategy: drops and recreates table on each run
# Suitable for staging layer that processes fresh data from ingestion
materialization:
  type: table
  strategy: create+replace

# Define output schema with quality checks
columns:
  - name: trip_id
    type: string
    description: Unique trip identifier (deduplicated)
    primary_key: true
    checks:
      - name: not_null
      - name: unique
  - name: vendor_id
    type: integer
    description: Taxi vendor identifier
    checks:
      - name: not_null
  - name: pickup_datetime
    type: timestamp
    description: Trip start time
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: Trip end time
    checks:
      - name: not_null
  - name: passenger_count
    type: integer
    description: Number of passengers
  - name: trip_distance
    type: double
    description: Trip distance in miles
    checks:
      - name: not_null
      - name: non_negative
  - name: rate_code_id
    type: integer
    description: Rate code classification
  - name: store_and_fwd_flag
    type: string
    description: Store and forward flag (Y/N)
  - name: pickup_location_id
    type: integer
    description: TLC zone for pickup
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: TLC zone for dropoff
    checks:
      - name: not_null
  - name: payment_type
    type: integer
    description: Payment method (FK to payment_lookup)
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment type
    checks:
      - name: not_null
  - name: fare_amount
    type: double
    description: Base fare
    checks:
      - name: not_null
      - name: non_negative
  - name: extra
    type: double
    description: Miscellaneous extra charges
  - name: mta_tax
    type: double
    description: MTA tax
  - name: tip_amount
    type: double
    description: Tip amount
  - name: tolls_amount
    type: double
    description: Toll charges
  - name: total_amount
    type: double
    description: Total trip fare
    checks:
      - name: not_null
      - name: non_negative
  - name: taxi_type
    type: string
    description: Type of taxi (yellow, green, etc.)
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: Data extraction timestamp



@bruin */


-- Main query: deduplicate trips, join with payment lookup, and validate schema
SELECT
  t.trip_id,
  t.vendor_id,
  t.pickup_datetime,
  t.dropoff_datetime,
  COALESCE(ABS(t.passenger_count), 1) AS passenger_count,
  t.trip_distance,
  t.rate_code_id,
  t.store_and_fwd_flag,
  t.pickup_location_id,
  t.dropoff_location_id,
  t.payment_type,
  COALESCE(pl.payment_type_name, 'unknown') AS payment_type_name,
  ABS(t.fare_amount) AS fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  ABS(t.total_amount) AS total_amount,
  t.taxi_type,
  t.extracted_at
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY extracted_at DESC) AS rn
  FROM ingestion.trips
  WHERE trip_id IS NOT NULL
    AND vendor_id IS NOT NULL
    AND pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND payment_type IS NOT NULL
    AND total_amount IS NOT NULL
    AND dropoff_datetime >= pickup_datetime
) t
LEFT JOIN ingestion.payment_lookup pl
  ON t.payment_type = pl.payment_type_id
WHERE t.rn = 1
