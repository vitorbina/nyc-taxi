import os
import logging
from glob import glob
from pyspark.sql import SparkSession, functions as F

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER_URL)
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.jars", "/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.hive.metastore.sharedPrefixes", "org.apache.hadoop.fs.s3a,com.amazonaws,org.wildfly.openssl")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "2g")
        .enableHiveSupport()
        .getOrCreate()
    )

def get_parquet_output_path(output_dir: str) -> str:
    files = glob(os.path.join(output_dir, "part-*.parquet"))
    if not files:
        raise RuntimeError(f"No parquet file found in: {output_dir}")
    return files[0]


def build_map_column(col_name: str, mapping: dict) -> F.Column:
    column = F.lit(None).cast("string")
    for code, label in mapping.items():
        column = F.when(F.col(col_name) == code, label).otherwise(column)
    return column
