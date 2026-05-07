import logging

from utils.spark import get_spark

logger = logging.getLogger(__name__)

APP_NAME = "final_revenue_by_zone"


def compute_revenue_by_zone(bucket: str) -> None:
    logger.info("Computing revenue_by_zone from staging tables...")
    spark = get_spark(APP_NAME)
    try:
        spark.sql("""
            SELECT
                pickup_datetime,
                pickup_zone,
                pickup_borough,
                'yellow_taxi'   AS taxi_type,
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
                'green_taxi'    AS taxi_type,
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
                'high_volume_fhv'                                   AS taxi_type,
                base_passenger_fare                                  AS fare_amount,
                tip_amount,
                base_passenger_fare + COALESCE(tip_amount, 0)       AS total_amount
            FROM staging.high_volume_fhv
            WHERE pickup_zone IS NOT NULL
        """).write.mode("overwrite").parquet(f"s3a://{bucket}/final/revenue")

        row_count = spark.read.parquet(f"s3a://{bucket}/final/revenue").count()
        logger.info("Wrote %d rows to s3a://%s/final/revenue", row_count, bucket)
    finally:
        spark.stop()
