{{ config(location=lake_location('final/zones_geo/data.parquet')) }}

WITH revenue_by_zone AS (
    SELECT
        pickup_zone,
        COUNT(*)                        AS trip_count,
        ROUND(SUM(total_amount), 2)     AS total_revenue,
        ROUND(AVG(total_amount), 2)     AS avg_fare
    FROM {{ ref('revenue') }}
    GROUP BY pickup_zone
)

SELECT
    z.location_id,
    z.borough,
    z.zone,
    z.geometry_json,
    COALESCE(r.trip_count, 0)       AS trip_count,
    COALESCE(r.total_revenue, 0)    AS total_revenue,
    COALESCE(r.avg_fare, 0)         AS avg_fare
FROM {{ source('reference', 'taxi_zones_geo') }} z
LEFT JOIN revenue_by_zone r ON z.zone = r.pickup_zone
