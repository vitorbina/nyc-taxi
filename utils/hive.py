import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "hive_setup"
DATABASE = "nyc_taxi"
BUCKET = "data-lake-nyc"


def repair_table(table: str):
    spark = get_spark(f"hive_repair_{table}")
    logger.info(f"Repairing partitions for {DATABASE}.{table}...")
    spark.sql(f"MSCK REPAIR TABLE {DATABASE}.{table}")
    spark.stop()
    logger.info(f"Partitions updated for {DATABASE}.{table}.")


def setup_hive(tables: list):
    spark = get_spark(APP_NAME)

    logger.info(f"Creating database {DATABASE}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    for table in tables:
        location = f"s3a://{BUCKET}/staging/{table}/"

        drop_sql = f"DROP TABLE IF EXISTS {DATABASE}.{table}"
        logger.info(f"Dropping table if exists: {drop_sql}")
        spark.sql(drop_sql)

        create_sql = f"""
            CREATE EXTERNAL TABLE {DATABASE}.{table}
            USING PARQUET
            LOCATION '{location}'
        """
        logger.info(f"Creating table: {create_sql.strip()}")
        spark.sql(create_sql)

        logger.info(f"Table {DATABASE}.{table} registered at {location}.")

    spark.stop()
    logger.info("Hive setup completed.")
