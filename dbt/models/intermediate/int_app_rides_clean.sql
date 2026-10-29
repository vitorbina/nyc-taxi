SELECT *
FROM {{ ref('stg_app_rides') }}
WHERE {{ clean_trip_rules(fare_column=none, has_distance=false) }}
