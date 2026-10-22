{{ config(
    location=lake_location('staging/green_taxi'),
    partition_by='partition_date',
) }}

SELECT
    CAST(r.VendorID AS INT) AS vendor_id,
    CASE CAST(r.VendorID AS INT)
        WHEN 1 THEN 'Creative Mobile Technologies, LLC'
        WHEN 2 THEN 'Curb Mobility, LLC'
        WHEN 6 THEN 'Myle Technologies Inc'
        WHEN 7 THEN 'Helix'
    END AS vendor_name,
    CAST(r.lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(r.lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    {{ elapsed_minutes('CAST(r.lpep_pickup_datetime AS TIMESTAMP)', 'CAST(r.lpep_dropoff_datetime AS TIMESTAMP)') }} AS trip_duration_minutes,
    CAST(r.passenger_count AS INT) AS passenger_count,
    CAST(r.trip_distance AS DOUBLE) AS trip_distance_miles,
    CASE CAST(r.RatecodeID AS INT)
        WHEN 1 THEN 'Standard rate'
        WHEN 2 THEN 'JFK'
        WHEN 3 THEN 'Newark'
        WHEN 4 THEN 'Nassau or Westchester'
        WHEN 5 THEN 'Negotiated fare'
        WHEN 6 THEN 'Group ride'
        WHEN 99 THEN 'Null/unknown'
    END AS rate_code_name,
    CAST(r.store_and_fwd_flag AS VARCHAR) AS store_and_fwd_flag,
    CASE CAST(r.trip_type AS INT)
        WHEN 1 THEN 'Street-hail'
        WHEN 2 THEN 'Dispatch'
    END AS trip_type_name,
    pz.zone AS pickup_zone,
    pz.borough AS pickup_borough,
    dz.zone AS dropoff_zone,
    dz.borough AS dropoff_borough,
    CASE CAST(r.payment_type AS INT)
        WHEN 0 THEN 'Flex Fare trip'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
    END AS payment_type_name,
    CAST(r.fare_amount AS DOUBLE) AS fare_amount,
    CAST(r.extra AS DOUBLE) AS extra,
    CAST(r.mta_tax AS DOUBLE) AS mta_tax,
    CAST(r.tip_amount AS DOUBLE) AS tip_amount,
    CAST(r.tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(r.improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    CAST(r.congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    CAST(r.total_amount AS DOUBLE) AS total_amount,
    r.partition_date
FROM {{ source('raw', 'green_taxi') }} r
LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} pz ON CAST(r.PULocationID AS INT) = pz.location_id
LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} dz ON CAST(r.DOLocationID AS INT) = dz.location_id
WHERE CAST(r.lpep_pickup_datetime AS TIMESTAMP) IS NOT NULL
  AND CAST(r.lpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL
  AND {{ ny_is_after('CAST(r.lpep_dropoff_datetime AS TIMESTAMP)', 'CAST(r.lpep_pickup_datetime AS TIMESTAMP)') }}
  AND CAST(r.PULocationID AS INT) IS NOT NULL
  AND CAST(r.DOLocationID AS INT) IS NOT NULL
  AND CAST(r.trip_distance AS DOUBLE) > 0
  AND CAST(r.fare_amount AS DOUBLE) > 0
  AND CAST(r.total_amount AS DOUBLE) > 0
  AND (r.passenger_count IS NULL OR CAST(r.passenger_count AS INT) > 0)
