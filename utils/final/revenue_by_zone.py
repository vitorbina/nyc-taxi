import logging
from utils.spark import get_spark

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

APP_NAME = "final_revenue_by_zone"


def compute_revenue_by_zone(bucket: str):
    spark = get_spark(APP_NAME)

    spark.sql("""
        SELECT
            pickup_datetime,
            pickup_zone,
            pickup_borough,
            'yellow_taxi' AS taxi_type,
            fare_amount,
            tip_amount,
            total_amount
        FROM staging.yellow_taxi
        WHERE pickup_zone IS NOT NULL

        UNION ALL

        SELECT
            pickup_datetime,
            pickup_zone,
            pickup_borough,
            'green_taxi',
            fare_amount,
            tip_amount,
            total_amount
        FROM staging.green_taxi
        WHERE pickup_zone IS NOT NULL

        UNION ALL

        SELECT
            pickup_datetime,
            pickup_zone,
            pickup_borough,
            'high_volume_fhv',
            base_passenger_fare AS fare_amount,
            tip_amount,
            base_passenger_fare + COALESCE(tip_amount, 0) AS total_amount
        FROM staging.high_volume_fhv
        WHERE pickup_zone IS NOT NULL
    """).coalesce(1).write.mode("overwrite").parquet(f"s3a://{bucket}/final/revenue")

    logger.info(f"revenue written to s3a://{bucket}/final/revenue")
    spark.stop()
