import logging
import os
import tempfile
import shutil
from utils.s3 import upload_file, download_file
from utils.spark import get_spark, get_parquet_output_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_zones"


def transform_zones(input_path: str, output_dir: str) -> str:
    spark = get_spark(APP_NAME)

    spark.read.option("header", "true").csv(input_path).createOrReplaceTempView("raw")

    spark.sql("""
        SELECT
            CAST(LocationID AS INT)    AS location_id,
            CAST(Borough AS STRING)    AS borough,
            CAST(Zone AS STRING)       AS zone,
            CAST(service_zone AS STRING) AS service_zone
        FROM raw
        WHERE LocationID IS NOT NULL
          AND Borough IS NOT NULL
          AND Zone IS NOT NULL
    """).coalesce(1).write.mode("overwrite").parquet(os.path.join(output_dir, "taxi_zone_lookup.parquet"))

    final_path = get_parquet_output_path(os.path.join(output_dir, "taxi_zone_lookup.parquet"))
    logger.info(f"Zones staging saved to: {final_path}")
    spark.stop()
    return final_path


def stage_zones(bucket: str):
    tmpfolder = tempfile.mkdtemp()
    try:
        file_name = "taxi_zone_lookup.csv"
        raw_key = f"raw/reference/taxi_zone_lookup/{file_name}"
        staging_key = "staging/reference/taxi_zone_lookup/taxi_zone_lookup.parquet"

        input_path = os.path.join(tmpfolder, "input", file_name)
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        download_file(bucket=bucket, key=raw_key, filepath=input_path)

        output_dir = os.path.join(tmpfolder, "output")
        os.makedirs(output_dir, exist_ok=True)
        output_path = transform_zones(input_path, output_dir)

        upload_file(filepath=output_path, bucket=bucket, key=staging_key)
        logger.info(f"Uploaded to {bucket}/{staging_key}")
    finally:
        shutil.rmtree(tmpfolder)
