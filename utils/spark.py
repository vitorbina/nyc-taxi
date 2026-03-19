import os
import logging
from glob import glob
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )


def get_parquet_output_path(output_dir: str) -> str:
    files = glob(os.path.join(output_dir, "part-*.parquet"))
    if not files:
        raise RuntimeError(f"No parquet file found in: {output_dir}")
    return files[0]
