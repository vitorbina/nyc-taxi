# NYC Taxi Ingestion

This pipeline handles the monthly extraction of trip records from the [NYC Taxi & Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The data is publicly available as Parquet files and covers three vehicle categories:

- **Yellow Taxi** — metered taxis operating primarily in Manhattan
- **Green Taxi** — street-hail liveries serving outer boroughs
- **FHV (App Rides)** — for-hire vehicles dispatched through apps
- **High Volume FHV** — Uber, Lyft and Via trips; the largest dataset, available from 2019

Each run downloads one file per category for the reference month and stores it in the data lake under a date-partitioned path:

```
raw/yellow_taxi/partition_date=2024-01-01/yellow_tripdata_2024-01.parquet
raw/green_taxi/partition_date=2024-01-01/green_tripdata_2024-01.parquet
raw/app_rides/partition_date=2024-01-01/fhv_tripdata_2024-01.parquet
raw/high_volume_fhv/partition_date=2024-01-01/fhvhv_tripdata_2024-01.parquet
```

The pipeline runs monthly (`@monthly`) starting from January 2024, with 2 retries on failure.

## References

- [Trip Record User Guide](https://www.nyc.gov/assets/tlc/downloads/pdf/trip_record_user_guide.pdf)
- [Yellow Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
- [Green Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)
- [FHV Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf)
- [High Volume FHV Trips Data Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_hvfhv.pdf)

## Data availability

NYC TLC publishes data with a 2 to 3-month lag. If a task shows as **Skipped**, it means the file isn't on the government servers yet — this is expected and not a pipeline error. The pipeline detects HTTP 403/404 responses and skips gracefully instead of failing.
