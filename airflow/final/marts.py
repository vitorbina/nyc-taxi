"""
Pre-aggregated tables for the dashboard (the "mart" end of the final layer).

The dashboard asks a fixed set of questions. Answering them from final.trips
means scanning ~290M rows per chart on every load (60-90s each in Trino). These
tables hold the same answers summed once by Spark, so each chart reads a few
million rows (trips_hourly) or 365 rows (weather_daily) instead.

trips_hourly keeps additive measures only (counts and sums). Ratios such as
average fare or speed are computed in the BI layer from these sums
(SUM(revenue) / SUM(fare_trips), ...), which keeps them exact for any filter
or grouping a viewer applies.
"""

import logging

from utils.spark import get_spark
from utils.paths import final_key, s3a

logger = logging.getLogger(__name__)


def _write(spark, df, bucket: str, table: str) -> None:
    path = s3a(bucket, final_key(table))
    df.write.mode("overwrite").parquet(path)
    logger.info("Wrote %d rows to %s", spark.read.parquet(path).count(), path)


def compute_trips_hourly(bucket: str) -> None:
    """One row per pickup hour x service x pickup zone."""
    spark = get_spark("final_trips_hourly")
    try:
        df = spark.sql("""
            SELECT
                date_trunc('HOUR', pickup_datetime)          AS pickup_hour,
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
            FROM final.trips
            GROUP BY 1, 2, 3, 4
        """)
        _write(spark, df, bucket, "trips_hourly")
    finally:
        spark.stop()


def compute_weather_daily(bucket: str) -> None:
    """One row per day: that day's weather and total trips (all services)."""
    spark = get_spark("final_weather_daily")
    try:
        df = spark.sql("""
            SELECT
                date,
                CASE WHEN dayofweek(date) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
                MAX(weather_description)     AS weather_description,
                MAX(avg_temperature_c)       AS avg_temperature_c,
                MAX(total_precipitation_mm)  AS total_precipitation_mm,
                MAX(avg_wind_speed_kmh)      AS avg_wind_speed_kmh,
                COUNT(*)                     AS trips
            FROM final.weather_impact
            GROUP BY date
        """)
        _write(spark, df, bucket, "weather_daily")
    finally:
        spark.stop()
