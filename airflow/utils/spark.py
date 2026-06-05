import os
import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def get_spark(app_name: str) -> SparkSession:
    logger.info("Creating Spark session: %s", app_name)
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "local[*]"))
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", os.getenv("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"))
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.hive.metastore.sharedPrefixes", "org.apache.hadoop.fs.s3a,com.amazonaws,org.wildfly.openssl")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "4g")
        # Adaptive Query Execution: lets Spark resize shuffle partitions at runtime,
        # which keeps per-task memory low and spills to disk instead of OOMing on large inputs.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .enableHiveSupport()
        .getOrCreate()
    )
