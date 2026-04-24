import os
import logging

from utils.spark import get_spark

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
    except Exception as e:
        logger.warning("Skipping repair for %s.%s — table not registered yet: %s", database, table, e)
    finally:
        spark.stop()


def setup_hive(tables: list, database: str, location_prefix: str, bucket: str = _DEFAULT_BUCKET) -> None:
    spark = get_spark("hive_setup")

    logger.info("Creating database %s...", database)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    try:
        for table in tables:
            location = f"s3a://{bucket}/{location_prefix}/{table}/"

            try:
                fields = spark.read.parquet(location).schema.fields
            except Exception:
                logger.warning("No data at %s — skipping %s.%s", location, database, table)
                continue

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

            logger.info("Creating table: %s", create_sql.strip())
            spark.sql(create_sql)
            logger.info("Table %s.%s registered at %s.", database, table, location)

            try:
                spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
                logger.info("Partitions repaired for %s.%s.", database, table)
            except Exception as e:
                logger.warning("Repair skipped for %s.%s — no data yet: %s", database, table, e)
    finally:
        spark.stop()

    logger.info("Hive setup completed for database %s.", database)
