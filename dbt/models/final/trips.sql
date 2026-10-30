{{ config(location=lake_location('final/trips'), partition_by='partition_date') }}

SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    'yellow_taxi'       AS taxi_type,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    trip_distance_miles,
    total_amount        AS fare_amount,
    passenger_count,
    partition_date
FROM {{ ref('int_yellow_taxi_clean') }}
WHERE pickup_borough IS NOT NULL

UNION ALL

SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    'green_taxi'        AS taxi_type,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    trip_distance_miles,
    total_amount        AS fare_amount,
    passenger_count,
    partition_date
FROM {{ ref('int_green_taxi_clean') }}
WHERE pickup_borough IS NOT NULL

UNION ALL

SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    'app_rides'         AS taxi_type,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    NULL                AS trip_distance_miles,
    NULL                AS fare_amount,
    NULL                AS passenger_count,
    partition_date
FROM {{ ref('int_app_rides_clean') }}
WHERE pickup_borough IS NOT NULL

UNION ALL

SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    'high_volume_fhv'   AS taxi_type,
    pickup_zone,
    pickup_borough,
    dropoff_zone,
    dropoff_borough,
    trip_distance_miles,
    -- total_amount for yellow/green already includes tips; add them for FHV too
    base_passenger_fare + COALESCE(tip_amount, 0) AS fare_amount,
    NULL                AS passenger_count,
    partition_date
FROM {{ ref('int_high_volume_fhv_clean') }}
WHERE pickup_borough IS NOT NULL
