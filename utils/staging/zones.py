import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_zones"


def stage_zones(bucket: str):
    raw_path = f"s3a://{bucket}/raw/reference/taxi_zone_lookup/taxi_zone_lookup.csv"
    staging_path = f"s3a://{bucket}/staging/reference/taxi_zone_lookup/taxi_zone_lookup.parquet"

    spark = get_spark(APP_NAME)
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

    logger.info(f"Zones staging written to {staging_path}")
    spark.stop()
