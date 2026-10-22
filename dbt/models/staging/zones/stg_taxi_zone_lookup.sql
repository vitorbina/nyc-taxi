{{ config(
    location=lake_location('staging/reference/taxi_zone_lookup/data.parquet'),
) }}

SELECT
    CAST(LocationID AS INT)       AS location_id,
    CAST(Borough AS VARCHAR)      AS borough,
    CAST(Zone AS VARCHAR)         AS zone,
    CAST(service_zone AS VARCHAR) AS service_zone
FROM {{ source('raw', 'taxi_zone_lookup') }}
WHERE LocationID IS NOT NULL
  AND Borough IS NOT NULL
  AND Zone IS NOT NULL
