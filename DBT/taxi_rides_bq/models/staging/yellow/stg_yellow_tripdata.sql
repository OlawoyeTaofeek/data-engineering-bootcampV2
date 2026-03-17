with tripdata as (
    SELECT *,
           row_number() OVER (PARTITION BY vendorid, tpep_pickup_datetime ORDER BY tpep_pickup_datetime) AS row_num
    FROM {{ source('staging', 'yellow_tripdata') }}
    WHERE vendorid IS NOT NULL
)
SELECT 
    -- identifier
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    CAST(vendorid AS INTEGER) AS vendor_id,
    {{ safe_cast('ratecodeid', 'INTEGER') }} AS rate_code_id,
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
    {{ safe_cast('payment_type', 'INTEGER') }} AS payment_type
FROM tripdata
WHERE row_num = 1

{% if var('is_test_run', default=false) %}
   limit 100
{% endif %}  