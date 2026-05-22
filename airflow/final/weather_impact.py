import logging

from utils.spark import get_spark

logger = logging.getLogger(__name__)

APP_NAME = "final_weather_impact"


def compute_weather_impact(bucket: str) -> None:
    logger.info("Computing weather_impact from staging tables...")
    spark = get_spark(APP_NAME)
    try:
        spark.sql("""
            SELECT
                TO_DATE(datetime)                    AS date,
                ROUND(AVG(temperature_c), 1)         AS avg_temperature_c,
                ROUND(SUM(precipitation_mm), 1)      AS total_precipitation_mm,
                ROUND(AVG(wind_speed_kmh), 1)        AS avg_wind_speed_kmh,
                MAX(CASE WHEN HOUR(datetime) = 12 THEN weather_description END) AS weather_description
            FROM staging.weather
            GROUP BY TO_DATE(datetime)
        """).createOrReplaceTempView("_weather_daily")

        spark.sql("""
            SELECT TO_DATE(pickup_datetime) AS date, total_amount AS fare_amount, 'yellow_taxi' AS taxi_type
            FROM staging.yellow_taxi
            UNION ALL
            SELECT TO_DATE(pickup_datetime) AS date, total_amount AS fare_amount, 'green_taxi' AS taxi_type
            FROM staging.green_taxi
            UNION ALL
            SELECT TO_DATE(pickup_datetime) AS date, NULL AS fare_amount, 'app_rides' AS taxi_type
            FROM staging.app_rides
            UNION ALL
            SELECT TO_DATE(pickup_datetime) AS date, base_passenger_fare AS fare_amount, 'high_volume_fhv' AS taxi_type
            FROM staging.high_volume_fhv
        """).createOrReplaceTempView("_trips_for_weather")

        spark.sql("""
            SELECT
                t.date,
                t.taxi_type,
                t.fare_amount,
                w.weather_description,
                w.avg_temperature_c,
                w.total_precipitation_mm,
                w.avg_wind_speed_kmh
            FROM _trips_for_weather t
            LEFT JOIN _weather_daily w ON t.date = w.date
        """).write.mode("overwrite").parquet(f"s3a://{bucket}/final/weather_impact")

        row_count = spark.read.parquet(f"s3a://{bucket}/final/weather_impact").count()
        logger.info("Wrote %d rows to s3a://%s/final/weather_impact", row_count, bucket)
    finally:
        spark.stop()
