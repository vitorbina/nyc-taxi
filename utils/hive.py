import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "hive_setup"
DATABASE = "nyc_taxi"
RAW_DATABASE = "nyc_taxi_raw"
FINAL_DATABASE = "nyc_taxi_final"
BUCKET = "data-lake-nyc"


def repair_table(table: str, database: str = DATABASE):
    spark = get_spark(f"hive_repair_{table}")
    try:
        logger.info(f"Repairing partitions for {database}.{table}...")
        spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
        logger.info(f"Partitions updated for {database}.{table}.")
    except Exception as e:
        logger.warning(f"Skipping repair for {database}.{table} — table not registered yet: {e}")
    finally:
        spark.stop()


def setup_hive(tables: list, database: str = DATABASE, location_prefix: str = "staging"):
    spark = get_spark(APP_NAME)

    logger.info(f"Creating database {database}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    for table in tables:
        location = f"s3a://{BUCKET}/{location_prefix}/{table}/"

        try:
            fields = spark.read.parquet(location).schema.fields
        except Exception:
            logger.warning(f"No data at {location} — skipping {database}.{table}")
            continue

        partition_col = next((f for f in fields if f.name == "partition_date"), None)
        regular_fields = [f for f in fields if f.name != "partition_date"]
        schema_ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in regular_fields)

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
        logger.info(f"Creating table: {create_sql.strip()}")
        spark.sql(create_sql)

        logger.info(f"Table {database}.{table} registered at {location}.")
        try:
            spark.sql(f"MSCK REPAIR TABLE {database}.{table}")
            logger.info(f"Partitions repaired for {database}.{table}.")
        except Exception as e:
            logger.warning(f"Repair skipped for {database}.{table} — no data yet: {e}")

    spark.stop()
    logger.info(f"Hive setup completed for database {database}.")
