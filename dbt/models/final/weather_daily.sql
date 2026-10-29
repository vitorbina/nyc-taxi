-- Dashboard mart: one row per day with that day's weather and total trips (all services)
{{ config(location=lake_location('final/weather_daily/data.parquet')) }}

SELECT
    date,
    -- dayofweek: 0 = Sunday ... 6 = Saturday
    CASE WHEN dayofweek(date) IN (0, 6) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    MAX(weather_description)     AS weather_description,
    MAX(avg_temperature_c)       AS avg_temperature_c,
    MAX(total_precipitation_mm)  AS total_precipitation_mm,
    MAX(avg_wind_speed_kmh)      AS avg_wind_speed_kmh,
    COUNT(*)                     AS trips
FROM {{ ref('weather_impact') }}
GROUP BY date
