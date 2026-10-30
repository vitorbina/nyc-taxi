# NYC Taxi Data Pipeline

Data engineering pipeline that ingests, processes, and serves NYC taxi trip records and daily weather data into a local Data Lake.

Medallion architecture (raw → staging → final) with Airflow orchestrating, dbt + DuckDB transforming via SQL, MinIO as storage, Hive Metastore as the catalog, and Trino + Superset as the consumption layer.

## Architecture

```
Sources (NYC TLC, Open-Meteo)
  ↓
Raw (MinIO) ── raw files, no transformation
  ↓
Staging (MinIO) ── cleaning, type casting, joins, code translations (dbt + DuckDB)
  ↓
Final (MinIO) ── unified and enriched tables ready for BI queries (dbt + DuckDB)
  ↓
Hive Metastore ── catalog for raw, staging and final layers (registered through Trino)
  ↓
Trino ── SQL engine that queries MinIO via Hive Metastore
  ↓
Superset ── BI dashboards connected to Trino
```

DuckDB is only the engine: it runs inside the Airflow task, reads Parquet/JSON/CSV straight
from MinIO and streams the result back to MinIO as Parquet. Nothing is stored in DuckDB itself.
Every model is tested by dbt right after it is built, and a table reaches the catalog (and the
dashboards) only if its tests pass.

## Stack

| | |
|---|---|
| Airflow | 3.1.7 |
| dbt-core / dbt-duckdb | 1.12.6 / 1.11.0 |
| DuckDB | 1.5.6 |
| Astronomer Cosmos | 1.15.1 |
| Hive Metastore | 4.0.0 |
| Trino | 483 |
| Superset | 6.0.0 |
| MinIO | RELEASE.2026-08-04T00-00-00Z (`pgsty/minio` community build) |
| PostgreSQL | 18.6 |
| Docker | 29.2.1 |
| Docker Compose | 5.1.0 |

Every version is pinned. The rule: the latest stable patch of the most mature line, never a
pre-release or a release younger than ~2 weeks.

## Prerequisites

- Docker and Docker Compose
- wget
- **~16 GB of RAM available to Docker.** On **Windows + WSL2**, Docker is capped at ~50% of the machine's RAM by default — raise it by creating `C:\Users\<user>\.wslconfig`:

  ```ini
  [wsl2]
  memory=16GB
  ```

  Then run `wsl --shutdown` in PowerShell and reopen the terminal. dbt runs inside the Airflow scheduler, whose container is capped at 4 GB: models are built one month at a time, so the memory they need doesn't grow with the history (see [Performance tuning](#performance-tuning)).

## Getting started

```bash
git clone <repo-url>
cd nyc-taxi

# copy and fill in credentials
cp .env.example .env
```

Open `.env` and replace all `changeme` values. MinIO has minimum requirements:
- `MINIO_ROOT_USER` — at least 3 characters
- `MINIO_ROOT_PASSWORD` — at least 8 characters

```bash
# initial setup (JDBC driver, Hive config, MinIO bucket)
sudo ./setup.sh

# start the infrastructure
docker compose up -d
```

First build takes a few minutes (Hive jars from Maven, dbt and Cosmos from PyPI). The `PYTHONPATH` warning that shows up is harmless.

Wait for everything to come up:

```bash
docker compose ps
```

### Airflow password

Auto-generated on first start:

```bash
docker exec nyc_airflow_webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

Password persists across restarts. Only changes if you tear down volumes (`docker compose down -v`).

### UIs

| Service | URL | Login |
|---|---|---|
| Airflow | localhost:8080 | admin / (password above) |
| MinIO | localhost:9001 | values from .env |
| Trino | localhost:8082 | — |
| Superset | localhost:3000 | admin / SUPERSET_ADMIN_PASSWORD from .env |

## Loading data

The first load is a **backfill**: you pick the date range and load the whole history in one go. After that, the pipeline runs on its own via schedule and assets (see [Recurring runs](#recurring-runs)).

Once the stack is up, run the backfill with a date range (`FROM_DATE TO_DATE`, first day of each month):

```bash
./scripts/backfill.sh 2025-04-01 2026-04-01
```

> **Just want sample data?** Run `./scripts/seed.sh` — it loads a fixed 3-month window (Jan–Mar 2025) so you can explore the dashboards without picking a range.

The script drives the whole load from the command line — no need to open the UI — waiting for each stage to finish before starting the next, and aborting if any DAG run fails:

1. **Reference data (zones)** — zone lookup and shapefile, needed by staging for enrichment.
2. **Pause taxi/weather staging** so the backfill doesn't trigger them once per asset event.
3. **Backfill ingestion** for the date range (taxi + weather).
4. **Stage weather** — must run before taxi staging, since `taxi_final` reads `staging.weather` but only waits on the taxi assets.
5. **Stage taxi** — builds every month missing from staging, one month at a time; `taxi_final` then fires automatically via assets and does the same.
6. **Register the raw catalog** (`hive_setup_raw`).

A few things about how it works:
- DAGs are created paused, and **a paused DAG won't run even when its asset fires** — the script unpauses each one before use.
- `--dag-run-conf '{"skip_repair": true}'` tells the ingestion's `update_hive` task to skip the per-run Hive repair (no catalog sync per month). Staging reads raw files straight from their MinIO path (not from the Hive `raw.*` tables), so it works even though raw isn't registered yet. The raw catalog is registered once at the end.
- Backfills have their own concurrency control, separate from the DAG's `max_active_runs`. Without `--max-active-runs`, all runs fire at once and the source CDNs return HTTP 403 (rate limiting); the script passes `--max-active-runs 2` to throttle it.
- Staging is paused during ingestion so it doesn't fire once per asset event; it runs once at the end and picks up every month the backfill added.

The `hive_setup_raw`, `hive_setup_staging` and `hive_setup_final` DAGs are also available on their own as operational tools to recreate the tables of a given layer when the schema changes.

## Recurring runs

After the initial load, the pipeline runs end-to-end on its own — ingestion on a schedule, staging and final layer via assets. No manual steps:

| DAG | Schedule | |
|---|---|---|
| `taxi_ingestion` | monthly | downloads Parquet files for 4 taxi types |
| `weather_ingestion` | daily | downloads hourly weather from Open-Meteo (one JSON Lines file per day) |
| `taxi_staging` | asset-triggered | runs automatically after all 4 raw taxi assets update; builds only new or changed months |
| `weather_staging` | asset-triggered | runs automatically after raw weather asset updates; rebuilds in full (small) |
| `taxi_final` | asset-triggered | runs automatically after all 4 staging taxi assets update; builds only new or changed months |

## Project structure

```
airflow/
  dags/                  DAG definitions (organized by domain: taxi, weather, zones, setup)
  staging/
    zones_geo.py         zone shapefile → polygons (geopandas; the one step SQL can't do)
  utils/
    assets.py            Airflow asset definitions (single source of truth)
    dbt_tasks.py         dbt models as Airflow tasks (Cosmos)
    incremental.py       which months a layer needs to build (compares files in MinIO)
    hive.py              Hive Metastore table registration through Trino
    s3.py                MinIO helpers (boto3)
    paths.py             S3 path construction helpers
dbt/
  models/staging/        raw → staging, one model per table (+ tests in _*__models.yml)
  models/intermediate/   int_*_clean: trip quality rules (ephemeral, inlined as CTEs)
  models/final/          final tables and dashboard marts (+ tests)
  macros/lake.sql        `lake` materialization: streams a model to Parquet on MinIO, month by month
  macros/time.sql        DST-aware durations for NYC wall-clock timestamps
  macros/quality.sql     trip quality rules, driven by vars in dbt_project.yml
  tests/                 generic test accepted_range + marts reconcile test
  profiles.yml           DuckDB in memory, MinIO credentials from env vars
dockerfile/
  Dockerfile.airflow     custom Airflow image (Cosmos + dbt in its own virtualenv)
  Dockerfile.hive        custom Hive image (S3A jars)
  Dockerfile.superset    custom Superset image (psycopg2 + Trino driver)
trino/
  etc/                   Trino configuration (config, jvm, catalog/hive)
superset/
  superset_config.py     Superset Python config (DB URI, secret key)
scripts/
  setup.sh               symlink to the root setup.sh (one-time env prep)
  backfill.sh            runs the full historical backfill for a date range
  seed.sh                loads a fixed 3-month window for quick demo/dev data
  export_dashboards.sh   zips dashboard YAMLs into dashboards/exports/
  import_dashboards.sh   imports the ZIP into Superset through its API
  deploy_dashboards.sh   deletes and reimports the charts, so chart edits in the YAML take effect
dashboards/
  nyc_taxi_overview/     dashboard YAML files (versioned)
  exports/               generated ZIPs for Superset import (not versioned)
setup.sh                 initial setup script
docker-compose.yml       full infrastructure
setup/                   local files generated by setup.sh (not versioned)
  hive-lib/              JDBC driver
  hive-conf/             Hive S3A credentials
```

## Transformations (dbt)

Staging and final are dbt models (`dbt/`), run by DuckDB. In Airflow, [Cosmos](https://github.com/astronomer/astronomer-cosmos)
turns each selected model into a task, followed by a task with that model's tests; a failing test
stops everything downstream, including the catalog registration. Tests that span several models
(`marts_reconcile_with_trips`) run as their own task after all of them.

Each dbt run starts with an empty in-memory DuckDB, so a model that refs another one reads it back
from MinIO: the `on-run-start` hook in `macros/lake.sql` creates those views for the upstream models
that aren't part of the run. That is what lets Airflow run one model per task.

### Incremental by month

Trip tables are partitioned by `partition_date`, the month of the TLC file, in staging and in final
(`trips`, `revenue`, `weather_impact`, `trips_hourly`). A run builds only the months that changed:

1. A `plan` task (`airflow/utils/incremental.py`) compares the files in MinIO: a month is stale when
   the target doesn't have it yet, or when its source was written after it (a new month, a
   re-downloaded one, or a month rebuilt upstream). No stale month → the rest is skipped.
2. dbt gets those months as the `partitions` var. The `lake` materialization filters the model's
   SELECT to one month and writes just that partition, once per month; DuckDB pushes the filter down
   to the folders it reads, so each step reads one month of input.
3. `zones_geo` and `weather_daily` are small and are recomputed whole from the monthly tables.

Because the work unit is one month, DuckDB's memory stays the same whether the lake holds one year
or ten. To rebuild everything (after changing a model's logic), trigger `taxi_staging` or
`taxi_final` with `full_refresh` checked; it still goes month by month.

Running dbt by hand, outside Airflow (from the repo root, with a virtualenv on Python 3.12):

```bash
pip install -r dbt/requirements.txt
set -a; source .env; set +a                  # MinIO credentials and bucket
cd dbt
dbt build --profiles-dir .                   # everything: run + test, in dependency order
dbt build --profiles-dir . -s stg_weather    # one model and its tests
# only some months of the partitioned models (without it, a partitioned model rebuilds in full):
dbt build --profiles-dir . -s stg_yellow_taxi --vars '{partitions: "2025-11-01,2025-12-01"}'
# write somewhere else instead of the real lake:
dbt build --profiles-dir . --vars '{output_lake: s3://data-lake-nyc/sandbox}'
```

Outside Docker, `MINIO_ENDPOINT` defaults to `http://localhost:9000`.

### Spark → dbt + DuckDB

Staging and final used to run on Spark (a master and two 4.5 GB workers, with the driver inside the
Airflow scheduler). Same machine (WSL2 VM, 16 vCPU, 15.8 GB RAM), same year of data (2025,
~292M trips), full rebuild through Airflow. Spark times are from its last full run, read from the
Airflow metadata database.

| | Spark 3.5 | dbt + DuckDB, whole table at once | dbt + DuckDB, month by month (current) |
|---|---|---|---|
| `taxi_staging`, full rebuild (4 tables) | 21.4 min | 6.5 min | **5.6 min** |
| `taxi_final`, full rebuild | 18.1 min (4 tables, no tests) | 5.1 min (6 tables + 21 tests) | **7.9 min** (6 tables + 21 tests) |
| `stg_high_volume_fhv` (~243M rows) | 915 s | 276 s | **200 s** |
| **A new month arrives** (staging + final) | ~40 min (everything rebuilt) | ~12 min (everything rebuilt) | **5.0 min** (2.0 + 3.0) |
| Memory limits for processing | 13.5 GB: 9.5 GB Spark containers + 4 GB scheduler (driver) | 7 GB scheduler | **4 GB scheduler** (peak measured 3.3 GB) |
| Containers | + spark-master, 2 workers | none extra | none extra |
| Registering a layer in the catalog | ~90 s (Spark session) | ~7 s (Trino) | ~7 s (Trino) |

Results match: identical row counts in every table, with one trip (out of 292M) and a few
durations differing around the DST switches, where DuckDB is the more correct one
(`trips_hourly` has all 8,760 hours of the year; Spark's dropped the spring-forward hour).
Going month by month changed no data: every staging partition is identical row by row, and so is
every final month (the `trips_hourly` sums differ only past the 9th decimal, from summing
floating-point values in a different order).

In the monthly run, about half of the time is fixed: starting dbt for each task and the tests,
which still scan the whole table. The work that depends on data is one month per table
(`stg_high_volume_fhv`: 30 s instead of 200 s).

## Performance tuning

dbt tasks run one at a time across all DAGs (Airflow pool `duckdb`, 1 slot, created by
`airflow-init`), so two DuckDB jobs never compete for memory. DuckDB is set in `docker-compose.yml`
by `DUCKDB_MEMORY_LIMIT` (1500MB; past it, DuckDB spills to disk instead of failing) and
`DUCKDB_THREADS` (4).

What drives DuckDB's memory here is threads × partitions being written at once, not data size:
each thread keeps its own write buffer per open partition, and those buffers can't spill. By
default DuckDB uses one thread per core (16 on this machine). Measured on `stg_high_volume_fhv`:

| | 16 threads | 4 threads |
|---|---|---|
| one month, 1 GB limit | out of memory | 26 s, process peak 1.6 GB |
| whole year at once, 1 GB limit | out of memory | out of memory (12 partitions open) |
| whole year at once, 4.5 GB limit | 261 s | — |

Going month by month keeps one partition open, so 4 threads fit in ~1.5 GB whatever the size of
the history. A manual `dbt build` without the `partitions` var still writes a whole partitioned
table at once, which needs a larger limit (the default outside Docker is 4 GB).

By default, Airflow uses its built-in parallelism limits. If tasks are queuing or running too slowly, tune these variables in `docker-compose.yml` under the `x-airflow-common` environment block:

```yaml
AIRFLOW__CORE__PARALLELISM: "8"           # max tasks running across all DAGs at once
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: "4"  # max tasks running per DAG
```

Restart the scheduler after changing:

```bash
docker restart nyc_airflow_scheduler
```

## Troubleshooting

### password authentication failed (Airflow or Superset)

Happens when the `.env` credentials were changed after the Postgres volumes were already created. The volume still holds the old password.

Remove the affected volumes and restart:

```bash
docker compose down
docker volume rm nyc-taxi_postgres_data nyc-taxi_postgres_superset_data
docker compose up -d
```

### Full reset (wipes all pipeline data)

```bash
docker compose down -v
docker compose up -d
```

Re-run `./setup.sh` and the pipeline from scratch after a full reset.

## Dashboards

The `dashboards/` directory contains Superset dashboards versioned as individual YAML files, organized by dashboard name. This allows `git diff` to show exactly which charts, datasets, or queries changed between versions.

```
dashboards/
    nyc_taxi_overview/
        metadata.yaml
        dashboards/
        charts/
        datasets/
        databases/
```

### Importing

Prerequisite: the pipeline has run at least once (see [Loading data](#loading-data) / `./scripts/backfill.sh`), so the
tables the charts read exist — `final.trips_hourly`, `final.weather_daily` and `final.zones_geo`.

Then, from the repo root:

```bash
./scripts/export_dashboards.sh                                   # zips dashboards/nyc_taxi_overview/
set -a; source .env; set +a                                      # exposes SUPERSET_ADMIN_PASSWORD
./scripts/import_dashboards.sh                                   # imports via the API (overwrite=true)
```

The ZIP carries its own Trino connection (`final`), so no database needs to be created by hand, and re-running
the import updates the existing dashboard instead of failing. Tested on a fresh Superset 6.0.0: all 10 charts
render. (The UI route — **Dashboards → Import** with `dashboards/exports/nyc_taxi_overview.zip` — works too.)

### Exporting

After making changes in Superset:

1. Go to **Dashboards → ... → Export** and save the ZIP to your machine.
2. Unzip it and flatten the timestamp subfolder:
   ```bash
   cd dashboards
   unzip /path/to/dashboard_export_*.zip -d tmp_export
   rm -rf nyc_taxi_overview
   mv tmp_export/dashboard_export_*/* nyc_taxi_overview/
   rm -rf tmp_export
   ```
3. Commit the updated YAML files — `git diff` will show exactly what changed.

| Dashboard | Description |
|---|---|
| `nyc_taxi_overview/` | Overview of 2025 (Jan–Dec, ~292M trips). KPIs: trips, revenue, average fare, revenue per mile. Charts: average trip speed by hour, hour × weekday demand heatmap, top 10 pickup zones by revenue, revenue choropleth by zone, share of trips by service, and the rain effect on daily trips (wet vs dry days, weekday/weekend). Each chart's subtitle states its composition (metric × dimension). Native filters for period, service and pickup borough. |

**Conventions used in the dashboard**

- **Revenue** = what the passenger paid incl. tips: `total_amount` for Yellow/Green, `base_passenger_fare + tips` for Uber/Lyft. "Other for-hire" (black cars, liveries — the TLC `fhv` file) publishes no fares or distances, so its revenue columns are N/A.
- **Services:** "Uber/Lyft" is the TLC high-volume FHV file (`fhvhv`); "Other for-hire" is the regular `fhv` file.
- **Times** are NYC local wall-clock, stored without a zone and read back as-is by Trino. Durations account for DST (`dbt/macros/time.sql`); the one unresolvable case is the repeated hour when clocks fall back.
- **Data quality:** `dbt/macros/quality.sql` drops trips outside their file's month, shorter than 1 min or longer than 4 h, longer than 100 mi, or with fares above 1,000 USD (~0.1% of records). The limits are vars in `dbt/dbt_project.yml`, shared with the tests.
- **Colors:** one hue for the whole dashboard — blue (`#2a78d6`) for volume and revenue, a light-to-dark blue scale for the heatmap and map (darker = more), and gray (`#8A8F98`) as the neutral and comparison color (e.g. axes, grid lines and secondary text). Contrast and color-blind separation were checked for every pair.
- **Map basemap:** Stadia Maps "Alidade Smooth" tiles (light gray, with street and neighborhood names; © Stadia Maps © OpenMapTiles © OpenStreetMap). Stadia serves `localhost` without an API key — a deployment on a real domain needs a free Stadia key. Hover a zone for its name, borough and revenue.
- **Theme:** Superset 6 follows the OS dark mode by default; `superset_config.py` pins the light theme because the palette was validated on a light surface.
- **Speed:** charts read two pre-aggregated mart tables built at the end of `taxi_final` (`dbt/models/final/trips_hourly.sql`, `weather_daily.sql`): `final.trips_hourly` (trips, revenue, distance and duration summed per hour × service × pickup zone — ~5M rows instead of ~290M) and `final.weather_daily` (one row per day). Ratios such as average fare or speed are computed in Superset from those sums, so they stay exact under any filter. The whole dashboard loads in ~6 s without cache (60–90 s per chart on the raw table).
- Chart results are cached for 24 h (`DATA_CACHE_CONFIG` in `superset/superset_config.py`); use **Force refresh** after re-running the pipeline.

## Connecting Superset to Trino

On first access to `localhost:3000`, log in with `admin` and the password set in `SUPERSET_ADMIN_PASSWORD`.

The dashboard import already creates the `final` connection. To explore the other layers in SQL Lab,
go to **Settings → Database Connections → Add database** and select **Trino**. Each medallion layer is exposed as a separate database so queries and dashboards are scoped to their layer. Add three connections:

| Display Name | SQLAlchemy URI |
|---|---|
| trino raw | `trino://admin@trino:8080/hive/raw` |
| trino staging | `trino://admin@trino:8080/hive/staging` |
| trino final | `trino://admin@trino:8080/hive/final` |
