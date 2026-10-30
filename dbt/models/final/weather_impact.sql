{{ config(location=lake_location('final/weather_impact'), partition_by='partition_date') }}

WITH weather_daily AS (
    SELECT
        CAST(datetime AS DATE)                AS date,
        ROUND(AVG(temperature_c), 1)          AS avg_temperature_c,
        ROUND(SUM(precipitation_mm), 1)       AS total_precipitation_mm,
        ROUND(AVG(wind_speed_kmh), 1)         AS avg_wind_speed_kmh,
        MAX(CASE WHEN hour(datetime) = 12 THEN weather_description END) AS weather_description
    FROM {{ ref('stg_weather') }}
    GROUP BY 1
),

trips_for_weather AS (
    SELECT CAST(pickup_datetime AS DATE) AS date, total_amount AS fare_amount, 'yellow_taxi' AS taxi_type, partition_date
    FROM {{ ref('int_yellow_taxi_clean') }}
    UNION ALL
    SELECT CAST(pickup_datetime AS DATE) AS date, total_amount AS fare_amount, 'green_taxi' AS taxi_type, partition_date
    FROM {{ ref('int_green_taxi_clean') }}
    UNION ALL
    SELECT CAST(pickup_datetime AS DATE) AS date, NULL AS fare_amount, 'app_rides' AS taxi_type, partition_date
    FROM {{ ref('int_app_rides_clean') }}
    UNION ALL
    SELECT CAST(pickup_datetime AS DATE) AS date, base_passenger_fare + COALESCE(tip_amount, 0) AS fare_amount, 'high_volume_fhv' AS taxi_type, partition_date
    FROM {{ ref('int_high_volume_fhv_clean') }}
)

SELECT
    t.date,
    t.taxi_type,
    t.fare_amount,
    w.weather_description,
    w.avg_temperature_c,
    w.total_precipitation_mm,
    w.avg_wind_speed_kmh,
    t.partition_date
FROM trips_for_weather t
LEFT JOIN weather_daily w ON t.date = w.date
