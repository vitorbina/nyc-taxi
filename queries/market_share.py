"""
Q2 - Market Share Evolution: Traditional Taxis vs Rideshare (Uber/Lyft)

Shows how Yellow/Green taxis lost ground to Uber, Lyft, and Via
over time, broken down by month.
"""

from queries.spark_minio import get_spark, curated

spark = get_spark("market_share")

spark.read.parquet(curated("yellow_taxi")).createOrReplaceTempView("yellow")
spark.read.parquet(curated("green_taxi")).createOrReplaceTempView("green")
spark.read.parquet(curated("high_volume_fhv")).createOrReplaceTempView("hvfhv")

result = spark.sql("""
    WITH all_trips AS (
        SELECT pickup_datetime, 'Yellow Taxi' AS service_type FROM yellow
        UNION ALL
        SELECT pickup_datetime, 'Green Taxi'  AS service_type FROM green
        UNION ALL
        SELECT pickup_datetime, company_name  AS service_type FROM hvfhv
    ),
    monthly AS (
        SELECT
            year(pickup_datetime)  AS year,
            month(pickup_datetime) AS month,
            service_type,
            COUNT(*) AS trip_count
        FROM all_trips
        GROUP BY year, month, service_type
    )
    SELECT
        year,
        month,
        service_type,
        trip_count,
        ROUND(trip_count * 100.0 / SUM(trip_count) OVER (PARTITION BY year, month), 2) AS market_share_pct
    FROM monthly
    ORDER BY year, month, service_type
""")

result.show(300, truncate=False)
