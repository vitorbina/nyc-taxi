-- The dashboard marts must add up to the trip-level table they summarize.
-- Returns a row (= failure) for any mart whose total drifts from final.trips.
WITH totals AS (
    SELECT 'trips_hourly' AS mart, (SELECT SUM(trips) FROM {{ ref('trips_hourly') }}) AS mart_trips
    UNION ALL
    SELECT 'weather_daily', (SELECT SUM(trips) FROM {{ ref('weather_daily') }})
)
SELECT mart, mart_trips, (SELECT COUNT(*) FROM {{ ref('trips') }}) AS trips
FROM totals
WHERE mart_trips <> (SELECT COUNT(*) FROM {{ ref('trips') }})
