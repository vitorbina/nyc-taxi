import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "final_weather_impact"


def compute_weather_impact(bucket: str):
    spark = get_spark(APP_NAME)

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

    spark.sql("""
        SELECT TO_DATE(pickup_datetime) AS date, total_amount AS fare_amount, 'yellow_taxi' AS taxi_type
        FROM staging.yellow_taxi
        UNION ALL
        SELECT TO_DATE(pickup_datetime), total_amount, 'green_taxi'
        FROM staging.green_taxi
        UNION ALL
        SELECT TO_DATE(pickup_datetime), NULL, 'app_rides'
        FROM staging.app_rides
        UNION ALL
        SELECT TO_DATE(pickup_datetime), base_passenger_fare, 'high_volume_fhv'
        FROM staging.high_volume_fhv
    """).createOrReplaceTempView("trips")

    spark.sql("""
        SELECT
            t.date,
            t.taxi_type,
            t.fare_amount,
            w.weather_description,
            w.avg_temperature_c,
            w.total_precipitation_mm,
            w.avg_wind_speed_kmh
        FROM trips t
        JOIN weather_daily w ON t.date = w.date
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/final/weather_impact")

    logger.info(f"weather_impact written to s3a://{bucket}/final/weather_impact")
    spark.stop()
