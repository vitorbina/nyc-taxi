# NYC Zone Reference Ingestion

The trip data from NYC TLC uses numeric IDs to identify pickup and dropoff locations (`PULocationID`, `DOLocationID`) and FHV dispatch bases. Without a reference file, those numbers don't mean much. This pipeline ingests the two lookup tables that make the data readable during analysis.

The files come from the same TLC CDN used for trip data and are ingested once (`@once`) since they're static reference tables that rarely change. If the TLC updates them, just re-trigger the DAG manually.

## What gets ingested

**`taxi_zone_lookup.csv`** — maps each `LocationID` to a borough, neighborhood name, and service zone. Essential for any geographic analysis across yellow taxi, green taxi, and FHV trips.

**`fhv_bases.csv`** — contains the licensed FHV dispatch bases: base number, name, and type. Useful when analyzing app-based ride volume by operator.

**`taxi_zones.zip`** — shapefile with the geographic polygon boundaries of each taxi zone. Stored as-is (raw ZIP) for future use in map-based analysis.

Files are stored without date partitioning since they're reference data, not time-series:

```
raw/reference/taxi_zone_lookup/taxi_zone_lookup.csv
raw/reference/fhv_bases/fhv_bases.csv
raw/reference/taxi_zones/taxi_zones.zip
```
