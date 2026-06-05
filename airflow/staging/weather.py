import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import folder_exists
from utils.spark import get_spark
from utils.paths import raw_key, staging_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "staging_weather"


def stage_weather(bucket: str) -> None:
    raw_prefix = raw_key("weather")
    raw_path = s3a(bucket, raw_prefix)
    staging_path = s3a(bucket, staging_key("weather"))

    if not folder_exists(bucket=bucket, prefix=raw_prefix):
        raise AirflowSkipException(f"No raw weather data found in MinIO: {raw_prefix}")

    logger.info("Staging weather — raw: %s", raw_path)

    spark = get_spark(APP_NAME)
    try:
        spark.read.json(raw_path).createOrReplaceTempView("raw")

        spark.sql("""
            SELECT
                CAST(time AS TIMESTAMP)                  AS datetime,
                CAST(temperature_2m AS DOUBLE)           AS temperature_c,
                CAST(apparent_temperature AS DOUBLE)     AS apparent_temperature_c,
                CAST(relative_humidity_2m AS INT)        AS humidity_pct,
                CAST(precipitation AS DOUBLE)            AS precipitation_mm,
                CAST(wind_speed_10m AS DOUBLE)           AS wind_speed_kmh,
                CAST(wind_direction_10m AS INT)          AS wind_direction_deg,
                CAST(weather_code AS INT)                AS weather_code,
                CASE CAST(weather_code AS INT)
                    WHEN 0  THEN 'Clear sky'
                    WHEN 1  THEN 'Mainly clear'
                    WHEN 2  THEN 'Partly cloudy'
                    WHEN 3  THEN 'Overcast'
                    WHEN 45 THEN 'Fog'
                    WHEN 48 THEN 'Rime fog'
                    WHEN 51 THEN 'Light drizzle'
                    WHEN 53 THEN 'Moderate drizzle'
                    WHEN 55 THEN 'Dense drizzle'
                    WHEN 61 THEN 'Slight rain'
                    WHEN 63 THEN 'Moderate rain'
                    WHEN 65 THEN 'Heavy rain'
                    WHEN 71 THEN 'Slight snow'
                    WHEN 73 THEN 'Moderate snow'
                    WHEN 75 THEN 'Heavy snow'
                    WHEN 77 THEN 'Snow grains'
                    WHEN 80 THEN 'Slight showers'
                    WHEN 81 THEN 'Moderate showers'
                    WHEN 82 THEN 'Violent showers'
                    WHEN 85 THEN 'Slight snow showers'
                    WHEN 86 THEN 'Heavy snow showers'
                    WHEN 95 THEN 'Thunderstorm'
                    WHEN 96 THEN 'Thunderstorm with hail'
                    WHEN 99 THEN 'Thunderstorm with heavy hail'
                END AS weather_description,
                partition_date
            FROM raw
        """).write.partitionBy("partition_date").mode("overwrite").parquet(staging_path)

        logger.info("Staged weather to %s", staging_path)
    finally:
        spark.stop()
