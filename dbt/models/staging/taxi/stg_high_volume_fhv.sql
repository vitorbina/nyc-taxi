{{ config(
    location=lake_location('staging/high_volume_fhv'),
    partition_by='partition_date',
) }}

SELECT
    CASE r.hvfhs_license_num
        WHEN 'HV0002' THEN 'Juno'
        WHEN 'HV0003' THEN 'Uber'
        WHEN 'HV0004' THEN 'Via'
        WHEN 'HV0005' THEN 'Lyft'
    END AS company_name,
    CAST(r.dispatching_base_num AS VARCHAR) AS dispatching_base_num,
    CAST(r.originating_base_num AS VARCHAR) AS originating_base_num,
    CAST(r.request_datetime AS TIMESTAMP) AS request_datetime,
    CAST(r.on_scene_datetime AS TIMESTAMP) AS on_scene_datetime,
    CAST(r.pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(r.dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    {{ elapsed_minutes('CAST(r.pickup_datetime AS TIMESTAMP)', 'CAST(r.dropoff_datetime AS TIMESTAMP)') }} AS trip_duration_minutes,
    CAST(r.trip_miles AS DOUBLE) AS trip_distance_miles,
    CAST(r.trip_time AS INT) AS trip_time_seconds,
    pz.zone AS pickup_zone,
    pz.borough AS pickup_borough,
    dz.zone AS dropoff_zone,
    dz.borough AS dropoff_borough,
    CAST(r.base_passenger_fare AS DOUBLE) AS base_passenger_fare,
    CAST(r.tolls AS DOUBLE) AS tolls_amount,
    CAST(r.bcf AS DOUBLE) AS black_car_fund,
    CAST(r.sales_tax AS DOUBLE) AS sales_tax,
    CAST(r.congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    CAST(r.airport_fee AS DOUBLE) AS airport_fee,
    CAST(r.tips AS DOUBLE) AS tip_amount,
    CAST(r.driver_pay AS DOUBLE) AS driver_pay,
    CAST(r.shared_request_flag AS VARCHAR) AS shared_request_flag,
    CAST(r.shared_match_flag AS VARCHAR) AS shared_match_flag,
    CAST(r.access_a_ride_flag AS VARCHAR) AS access_a_ride_flag,
    CAST(r.wav_request_flag AS VARCHAR) AS wav_request_flag,
    CAST(r.wav_match_flag AS VARCHAR) AS wav_match_flag,
    r.partition_date
FROM {{ source('raw', 'high_volume_fhv') }} r
LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} pz ON CAST(r.PULocationID AS INT) = pz.location_id
LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} dz ON CAST(r.DOLocationID AS INT) = dz.location_id
WHERE CAST(r.pickup_datetime AS TIMESTAMP) IS NOT NULL
  AND CAST(r.dropoff_datetime AS TIMESTAMP) IS NOT NULL
  AND {{ ny_is_after('CAST(r.dropoff_datetime AS TIMESTAMP)', 'CAST(r.pickup_datetime AS TIMESTAMP)') }}
  AND CAST(r.PULocationID AS INT) IS NOT NULL
  AND CAST(r.DOLocationID AS INT) IS NOT NULL
  AND CAST(r.trip_miles AS DOUBLE) > 0
  AND CAST(r.base_passenger_fare AS DOUBLE) > 0
