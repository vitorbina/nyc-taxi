import logging

from utils.spark import get_spark
from utils.s3 import folder_exists
from utils.constants import PARTITION_COL, STAGING_DATABASE, RAW_DATABASE, FINAL_DATABASE, DEFAULT_BUCKET

logger = logging.getLogger(__name__)


def _to_hive_type(spark_type: str) -> str:
    return spark_type.replace("timestamp_ntz", "timestamp")


def _table_exists(spark, database: str, table: str) -> bool:
    return spark.sql(f"SHOW TABLES IN {database} LIKE '{table}'").count() > 0


def _has_partitions(spark, database: str, table: str) -> bool:
    columns = spark.catalog.listColumns(table, dbName=database)
    return any(col.isPartition for col in columns)


def _create_external_table(spark, table: str, database: str, location_prefix: str, bucket: str) -> None:
    prefix = f"{location_prefix}/{table}/"
    location = f"s3a://{bucket}/{prefix}"

    if not folder_exists(bucket, prefix):
        raise FileNotFoundError(
            f"No data found at s3a://{bucket}/{prefix}. "
            f"Run the ingestion DAG for '{table}' before this task."
        )

    logger.info("Reading schema from %s...", location)
    fields = spark.read.parquet(location).schema.fields

    partition_col = next((f for f in fields if f.name == PARTITION_COL), None)
    regular_fields = [f for f in fields if f.name != PARTITION_COL]
    schema_ddl = ", ".join(
        f"`{f.name}` {_to_hive_type(f.dataType.simpleString())}"
        for f in regular_fields
    )

    if partition_col:
        create_sql = f"""
            CREATE EXTERNAL TABLE {database}.{table} ({schema_ddl})
            PARTITIONED BY ({PARTITION_COL} STRING)
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


def repair_table(table: str, database: str = STAGING_DATABASE, location_prefix: str = None, bucket: str = DEFAULT_BUCKET) -> None:
    if location_prefix is None:
        location_prefix = database

    spark = get_spark(f"hive_repair_{table}")
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        if not _table_exists(spark, database, table):
            logger.info("Table %s.%s does not exist, creating from parquet schema...", database, table)
            _create_external_table(spark, table, database, location_prefix, bucket)

        if _has_partitions(spark, database, table):
            logger.info("Repairing partitions for %s.%s...", database, table)
            spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
            logger.info("Partitions updated for %s.%s.", database, table)
    finally:
        spark.stop()


def setup_hive(tables: list, database: str, location_prefix: str, bucket: str = DEFAULT_BUCKET) -> None:
    spark = get_spark("hive_setup")
    try:
        logger.info("Creating database %s if not exists...", database)
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
            _create_external_table(spark, table, database, location_prefix, bucket)
            if _has_partitions(spark, database, table):
                logger.info("Repairing partitions for %s.%s...", database, table)
                spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
            logger.info("Table %s.%s registered successfully.", database, table)
    finally:
        spark.stop()
