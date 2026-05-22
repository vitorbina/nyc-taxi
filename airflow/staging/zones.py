import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import file_exists
from utils.spark import get_spark
from utils.paths import raw_key, staging_key, s3a

logger = logging.getLogger(__name__)

APP_NAME = "staging_zones"


ZONES_RAW_KEY = raw_key("reference/taxi_zone_lookup", file_name="taxi_zone_lookup.csv")
ZONES_STAGING_KEY = staging_key("reference/taxi_zone_lookup") + "/taxi_zone_lookup.parquet"


def stage_zones(bucket: str) -> None:
    if not file_exists(bucket=bucket, key=ZONES_RAW_KEY):
        raise AirflowSkipException(f"Raw file not found in MinIO: {ZONES_RAW_KEY}")

    logger.info("Staging zones — raw: %s", s3a(bucket, ZONES_RAW_KEY))

    spark = get_spark(APP_NAME)
    try:
        spark.read.option("header", "true").csv(s3a(bucket, ZONES_RAW_KEY)).createOrReplaceTempView("raw")

        spark.sql("""
            SELECT
                CAST(LocationID AS INT)      AS location_id,
                CAST(Borough AS STRING)      AS borough,
                CAST(Zone AS STRING)         AS zone,
                CAST(service_zone AS STRING) AS service_zone
            FROM raw
            WHERE LocationID IS NOT NULL
              AND Borough IS NOT NULL
              AND Zone IS NOT NULL
        """).coalesce(1).write.mode("overwrite").parquet(s3a(bucket, ZONES_STAGING_KEY))

        row_count = spark.read.parquet(s3a(bucket, ZONES_STAGING_KEY)).count()
        logger.info("Wrote %d rows to %s", row_count, s3a(bucket, ZONES_STAGING_KEY))
    finally:
        spark.stop()
