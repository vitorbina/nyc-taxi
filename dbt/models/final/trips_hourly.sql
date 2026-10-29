{#
  Dashboard mart: one row per pickup hour x service x pickup zone.

  Answering the dashboard from final.trips means scanning ~290M rows per chart on
  every load (60-90s each in Trino). This holds the same answers summed once, so
  each chart reads a few million rows instead. Only additive measures (counts and
  sums) are kept; ratios such as average fare or speed are computed in the BI layer
  (SUM(revenue) / SUM(fare_trips), ...), which keeps them exact for any filter.
#}
{{ config(location=lake_location('final/trips_hourly/data.parquet')) }}

SELECT
    date_trunc('hour', pickup_datetime)          AS pickup_hour,
    taxi_type,
    pickup_borough,
    pickup_zone,
    COUNT(*)                                     AS trips,
    SUM(fare_amount)                             AS revenue,
    COUNT(fare_amount)                           AS fare_trips,
    SUM(trip_distance_miles)                     AS distance_miles,
    -- duration only where distance is known, so distance / duration is a true speed
    SUM(CASE WHEN trip_distance_miles IS NOT NULL
             THEN trip_duration_minutes END)     AS duration_minutes_with_distance
FROM {{ ref('trips') }}
GROUP BY 1, 2, 3, 4
