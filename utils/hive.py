import os
import logging

from utils.spark import get_spark
from utils.s3 import folder_exists

logger = logging.getLogger(__name__)

STAGING_DATABASE = "staging"
RAW_DATABASE = "raw"
FINAL_DATABASE = "final"

_DEFAULT_BUCKET = os.getenv("MINIO_BUCKET")


def _to_hive_type(spark_type: str) -> str:
    return spark_type.replace("timestamp_ntz", "timestamp")


def repair_table(table: str, database: str = STAGING_DATABASE) -> None:
    spark = get_spark(f"hive_repair_{table}")
    try:
        logger.info("Repairing partitions for %s.%s...", database, table)
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info("Partitions updated for %s.%s.", database, table)
    finally:
        spark.stop()


def setup_hive(tables: list, database: str, location_prefix: str, bucket: str = _DEFAULT_BUCKET) -> None:
    spark = get_spark("hive_setup")

    try:
        logger.info("Creating database %s if not exists...", database)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        for table in tables:
            prefix = f"{location_prefix}/{table}/"
            location = f"s3a://{bucket}/{prefix}"

            if not folder_exists(bucket, prefix):
                raise FileNotFoundError(
                    f"No data found at s3a://{bucket}/{prefix}. "
                    f"Run the ingestion DAG for '{table}' before hive_setup."
                )

            logger.info("Reading schema from %s...", location)
            fields = spark.read.parquet(location).schema.fields

            partition_col = next((f for f in fields if f.name == "partition_date"), None)
            regular_fields = [f for f in fields if f.name != "partition_date"]
            schema_ddl = ", ".join(
                f"`{f.name}` {_to_hive_type(f.dataType.simpleString())}"
                for f in regular_fields
            )

            spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")

            if partition_col:
                create_sql = f"""
                    CREATE EXTERNAL TABLE {database}.{table} ({schema_ddl})
                    PARTITIONED BY (partition_date STRING)
                    STORED AS PARQUET
                    LOCATION '{location}'
                """
            else:
                create_sql = f"""
                    CREATE EXTERNAL TABLE {database}.{table} ({schema_ddl})
                    STORED AS PARQUET
                    LOCATION '{location}'
                """

            logger.info("Creating table %s.%s at %s", database, table, location)
            spark.sql(create_sql)

            if partition_col:
                logger.info("Repairing partitions for %s.%s...", database, table)
                spark.sql(f"MSCK REPAIR TABLE {database}.{table}")

            logger.info("Table %s.%s registered successfully.", database, table)
    finally:
        spark.stop()
