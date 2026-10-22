{{ config(
    location=lake_location('staging/app_rides'),
    partition_by='partition_date',
) }}

SELECT
    CAST(r.dispatching_base_num AS VARCHAR) AS dispatching_base_num,
    CAST(r.Affiliated_base_number AS VARCHAR) AS affiliated_base_num,
    CAST(r.pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(r.dropOff_datetime AS TIMESTAMP) AS dropoff_datetime,
    {{ elapsed_minutes('CAST(r.pickup_datetime AS TIMESTAMP)', 'CAST(r.dropOff_datetime AS TIMESTAMP)') }} AS trip_duration_minutes,
    CAST(r.SR_Flag AS VARCHAR) AS shared_ride_flag,
    pz.zone AS pickup_zone,
    pz.borough AS pickup_borough,
    dz.zone AS dropoff_zone,
    dz.borough AS dropoff_borough,
    r.partition_date
FROM {{ source('raw', 'app_rides') }} r
LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} pz ON CAST(r.PUlocationID AS INT) = pz.location_id
LEFT JOIN {{ ref('stg_taxi_zone_lookup') }} dz ON CAST(r.DOlocationID AS INT) = dz.location_id
WHERE CAST(r.pickup_datetime AS TIMESTAMP) IS NOT NULL
  AND CAST(r.dropOff_datetime AS TIMESTAMP) IS NOT NULL
  AND {{ ny_is_after('CAST(r.dropOff_datetime AS TIMESTAMP)', 'CAST(r.pickup_datetime AS TIMESTAMP)') }}
  AND r.dispatching_base_num IS NOT NULL
  AND CAST(r.PUlocationID AS INT) IS NOT NULL
  AND CAST(r.DOlocationID AS INT) IS NOT NULL
