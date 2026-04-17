import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "final_weather_impact"


def compute_weather_impact(bucket: str):
    """
    Joins daily trip counts with daily weather conditions.
    Weather is aggregated to daily using the noon snapshot (12:00) for the
    description and averages for numeric fields.
    Includes all taxi types; avg_fare is NULL for app_rides (no fare data).
    """
    spark = get_spark(APP_NAME)

    # Daily weather: use noon reading for description, averages for metrics
    spark.sql("""
        SELECT
            TO_DATE(datetime)                    AS date,
            ROUND(AVG(temperature_c), 1)         AS avg_temperature_c,
            ROUND(SUM(precipitation_mm), 1)      AS total_precipitation_mm,
            ROUND(AVG(wind_speed_kmh), 1)        AS avg_wind_speed_kmh,
            MAX(CASE WHEN HOUR(datetime) = 12 THEN weather_description END) AS weather_description
        FROM staging.weather
        GROUP BY TO_DATE(datetime)
    """).createOrReplaceTempView("weather_daily")

    # Daily trip counts — union all taxi types
    spark.sql("""
        SELECT TO_DATE(pickup_datetime) AS date, COUNT(*) AS trips, AVG(total_amount) AS fare
        FROM staging.yellow_taxi GROUP BY TO_DATE(pickup_datetime)
        UNION ALL
        SELECT TO_DATE(pickup_datetime), COUNT(*), AVG(total_amount)
        FROM staging.green_taxi GROUP BY TO_DATE(pickup_datetime)
        UNION ALL
        SELECT TO_DATE(pickup_datetime), COUNT(*), NULL
        FROM staging.app_rides GROUP BY TO_DATE(pickup_datetime)
        UNION ALL
        SELECT TO_DATE(pickup_datetime), COUNT(*), AVG(base_passenger_fare)
        FROM staging.high_volume_fhv GROUP BY TO_DATE(pickup_datetime)
    """).createOrReplaceTempView("trips_raw")

    spark.sql("""
        SELECT date, SUM(trips) AS total_trips, ROUND(AVG(fare), 2) AS avg_fare
        FROM trips_raw
        GROUP BY date
    """).createOrReplaceTempView("trips_daily")

    spark.sql("""
        SELECT
            w.date,
            w.weather_description,
            w.avg_temperature_c,
            w.total_precipitation_mm,
            w.avg_wind_speed_kmh,
            t.total_trips,
            t.avg_fare
        FROM weather_daily w
        JOIN trips_daily t ON w.date = t.date
        ORDER BY w.date
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/final/weather_impact")

    logger.info(f"weather_impact written to s3a://{bucket}/final/weather_impact")
    spark.stop()
