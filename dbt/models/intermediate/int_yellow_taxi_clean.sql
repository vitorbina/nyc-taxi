SELECT *
FROM {{ ref('stg_yellow_taxi') }}
WHERE {{ clean_trip_rules(fare_column='total_amount', has_distance=true) }}
