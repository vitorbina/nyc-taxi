"""
Trip-level quality rules shared by every final table.

Staging keeps each TLC file as published (typed and translated). The source
still contains records no BI consumer should see: a $863k yellow fare, trips of
276k miles, trips lasting days, and a few pickups dated outside the file's month.
These rules drop them once, so trips, revenue and weather_impact all agree.
"""

MAX_DISTANCE_MILES = 100
MAX_FARE_USD = 1000
MIN_DURATION_MINUTES = 1
MAX_DURATION_MINUTES = 240

# staging table -> column holding the fare used for revenue (None: not reported)
TRIP_TABLES = {
    "yellow_taxi": "total_amount",
    "green_taxi": "total_amount",
    "high_volume_fhv": "base_passenger_fare",
    "app_rides": None,
}


def _rules(fare_column: str | None, has_distance: bool) -> str:
    rules = [
        # pickup must fall inside the month of the file it came from
        "pickup_datetime >= CAST(partition_date AS TIMESTAMP)",
        "pickup_datetime < CAST(add_months(CAST(partition_date AS DATE), 1) AS TIMESTAMP)",
        f"trip_duration_minutes BETWEEN {MIN_DURATION_MINUTES} AND {MAX_DURATION_MINUTES}",
    ]
    if has_distance:
        rules.append(f"trip_distance_miles > 0 AND trip_distance_miles <= {MAX_DISTANCE_MILES}")
    if fare_column:
        rules.append(f"{fare_column} > 0 AND {fare_column} <= {MAX_FARE_USD}")
    return "\n  AND ".join(rules)


def register_clean_trip_views(spark) -> None:
    """Expose each staging trip table as a temp view of the same name with the rules applied.

    Final queries then read `FROM yellow_taxi` instead of `FROM staging.yellow_taxi`.
    """
    for table, fare_column in TRIP_TABLES.items():
        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW {table} AS
            SELECT * FROM staging.{table}
            WHERE {_rules(fare_column, has_distance=fare_column is not None)}
        """)
