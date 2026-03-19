import logging
import os
import tempfile
import shutil
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType
from utils.s3 import upload_file, download_file
from utils.spark import get_spark, get_parquet_output_path

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "staging_zones"


def transform_zones(input_path: str, output_dir: str) -> str:
    spark = get_spark(APP_NAME)

    df = spark.read.option("header", "true").csv(input_path)

    df = df.select(
        F.col("LocationID").cast(IntegerType()).alias("location_id"),
        F.col("Borough").cast(StringType()).alias("borough"),
        F.col("Zone").cast(StringType()).alias("zone"),
        F.col("service_zone").cast(StringType()),
    )

    df = df.filter(
        F.col("location_id").isNotNull() &
        F.col("borough").isNotNull() &
        F.col("zone").isNotNull()
    )

    file_name = "taxi_zone_lookup.parquet"
    output_path = os.path.join(output_dir, file_name)
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    final_path = get_parquet_output_path(output_path)
    logger.info(f"Zones staging saved to: {final_path}")
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
