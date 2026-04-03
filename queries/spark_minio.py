import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

BUCKET = "data-lake-nyc"
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "local[*]")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
# SPARK_S3A_ENDPOINT must point to MinIO as seen by the Spark executors.
# local[*]: executors run on host → use localhost:9000
# spark://spark-master:7077: executors run in Docker → use minio:9000
SPARK_S3A_ENDPOINT = os.getenv("SPARK_S3A_ENDPOINT", "http://localhost:9000")


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER_URL)
        .config("spark.sql.session.timeZone", "America/New_York")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", SPARK_S3A_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def curated(folder: str) -> str:
    return f"s3a://{BUCKET}/curated/{folder}/*/*.parquet"
