import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "hive_setup"
DATABASE = "nyc_taxi"
BUCKET = "data-lake-nyc"

TABLES = [
    "yellow_taxi",
    "green_taxi",
    "app_rides",
    "high_volume_fhv",
    "weather",
]


def repair_table(table: str):
    spark = get_spark(f"hive_repair_{table}")
    logger.info(f"Repairing partitions for {DATABASE}.{table}...")
    spark.sql(f"MSCK REPAIR TABLE {DATABASE}.{table}")
    spark.stop()
    logger.info(f"Partitions updated for {DATABASE}.{table}.")


def setup_hive():
    spark = get_spark(APP_NAME)

    logger.info(f"Creating database {DATABASE}...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")

    for table in TABLES:
        location = f"s3a://{BUCKET}/staging/{table}/"
        logger.info(f"Registering table {DATABASE}.{table} at {location}...")

        spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{table}")
        spark.sql(f"""
            CREATE TABLE {DATABASE}.{table}
            USING PARQUET
            LOCATION '{location}'
        """)

        logger.info(f"Table {DATABASE}.{table} registered.")

    spark.stop()
    logger.info("Hive setup completed.")
