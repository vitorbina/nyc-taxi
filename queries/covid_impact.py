"""
Q1 - COVID-19 Impact on Taxi Demand (2019-2022)

Compares monthly trip volume and revenue for yellow and green taxis
across the pre-COVID, lockdown, and recovery periods.
"""

from queries.spark_minio import get_spark, curated

spark = get_spark("covid_impact")

spark.read.parquet(curated("yellow_taxi")).createOrReplaceTempView("yellow")
spark.read.parquet(curated("green_taxi")).createOrReplaceTempView("green")

result = spark.sql("""
    SELECT
        year(pickup_datetime)  AS year,
        month(pickup_datetime) AS month,
        taxi_type,
        COUNT(*)               AS trip_count,
        ROUND(SUM(total_amount), 2)  AS total_revenue,
        ROUND(AVG(fare_amount), 2)   AS avg_fare
    FROM (
        SELECT pickup_datetime, total_amount, fare_amount, 'yellow' AS taxi_type FROM yellow
        UNION ALL
        SELECT pickup_datetime, total_amount, fare_amount, 'green'  AS taxi_type FROM green
    )
    WHERE year(pickup_datetime) BETWEEN 2019 AND 2022
    GROUP BY year, month, taxi_type
    ORDER BY year, month, taxi_type
""")

result.show(200, truncate=False)
