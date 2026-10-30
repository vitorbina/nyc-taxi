{{ config(location=lake_location('final/revenue'), partition_by='partition_date') }}

SELECT
    pickup_datetime,
    pickup_zone,
    pickup_borough,
    'yellow_taxi'   AS taxi_type,
    fare_amount,
    tip_amount,
    total_amount,
    partition_date
FROM {{ ref('int_yellow_taxi_clean') }}
WHERE pickup_zone IS NOT NULL

UNION ALL

SELECT
    pickup_datetime,
    pickup_zone,
    pickup_borough,
    'green_taxi'    AS taxi_type,
    fare_amount,
    tip_amount,
    total_amount,
    partition_date
FROM {{ ref('int_green_taxi_clean') }}
WHERE pickup_zone IS NOT NULL

UNION ALL

SELECT
    pickup_datetime,
    pickup_zone,
    pickup_borough,
    'high_volume_fhv'                               AS taxi_type,
    base_passenger_fare                             AS fare_amount,
    tip_amount,
    base_passenger_fare + COALESCE(tip_amount, 0)   AS total_amount,
    partition_date
FROM {{ ref('int_high_volume_fhv_clean') }}
WHERE pickup_zone IS NOT NULL
