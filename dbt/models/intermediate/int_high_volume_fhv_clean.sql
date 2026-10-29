SELECT *
FROM {{ ref('stg_high_volume_fhv') }}
WHERE {{ clean_trip_rules(fare_column='base_passenger_fare', has_distance=true) }}
