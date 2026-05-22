import logging

from airflow.exceptions import AirflowSkipException

from utils.s3 import file_exists
from utils.spark import get_spark

logger = logging.getLogger(__name__)

APP_NAME = "staging_zones"


def stage_zones(bucket: str) -> None:
    raw_key = "raw/reference/taxi_zone_lookup/taxi_zone_lookup.csv"
    raw_path = f"s3a://{bucket}/{raw_key}"
    staging_path = f"s3a://{bucket}/staging/reference/taxi_zone_lookup/taxi_zone_lookup.parquet"

    if not file_exists(bucket=bucket, key=raw_key):
        raise AirflowSkipException(f"Raw file not found in MinIO: {raw_key}")

    logger.info("Staging zones — raw: %s", raw_path)

    spark = get_spark(APP_NAME)
    try:
        spark.read.option("header", "true").csv(raw_path).createOrReplaceTempView("raw")

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
        """).coalesce(1).write.mode("overwrite").parquet(staging_path)

        row_count = spark.read.parquet(staging_path).count()
        logger.info("Wrote %d rows to %s", row_count, staging_path)
    finally:
        spark.stop()
