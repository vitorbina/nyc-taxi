# NYC Taxi Data Pipeline

Data engineering pipeline that ingests, processes, and serves NYC taxi trip records and daily weather data into a local Data Lake.

Medallion architecture (raw → staging → final) with Airflow orchestrating, Spark processing via SQL, MinIO as storage, Hive Metastore as the catalog, and Trino + Superset as the consumption layer.

## Architecture

```
Sources (NYC TLC, Open-Meteo)
  ↓
Raw (MinIO) ── raw files, no transformation
  ↓
Staging (MinIO) ── cleaning, type casting, joins, code translations (Spark SQL)
  ↓
Final (MinIO) ── unified and enriched tables ready for BI queries
  ↓
Hive Metastore ── catalog for raw, staging and final layers
  ↓
Trino ── SQL engine that queries MinIO via Hive Metastore
  ↓
Superset ── BI dashboards connected to Trino
```

## Stack

| | |
|---|---|
| Airflow | 3.1.7 |
| Spark | 3.5.3 |
| Hive Metastore | 4.0.0 |
| Trino | latest |
| Superset | latest |
| MinIO | latest |
| PostgreSQL | latest |
| Docker | 29.2.1 |
| Docker Compose | 5.1.0 |

## Prerequisites

- Docker and Docker Compose
- wget
- **~24 GB of RAM available to Docker.** The full stack (Spark, Trino, Hive, Airflow, Superset, MinIO, Postgres) is memory-heavy. On **Windows + WSL2**, Docker is capped at ~50% of the machine's RAM by default — raise it by creating `C:\Users\<user>\.wslconfig`:

  ```ini
  [wsl2]
  memory=24GB
  ```

  Then run `wsl --shutdown` in PowerShell and reopen the terminal. Without this, heavy staging jobs (especially `high_volume_fhv`) can exhaust memory.

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

First build takes a few minutes since it downloads jars from Maven. The `PYTHONPATH` warning that shows up is harmless.

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
| Spark | localhost:8081 | — |
| Trino | localhost:8082 | — |
| Superset | localhost:3000 | admin / SUPERSET_ADMIN_PASSWORD from .env |

## Loading data

The first load is a **backfill**: you pick the date range and load the whole history in one go. After that, the pipeline runs on its own via schedule and assets (see [Recurring runs](#recurring-runs)).

Once the stack is up, run the backfill with a date range (`FROM_DATE TO_DATE`, first day of each month):

```bash
./scripts/backfill.sh 2025-04-01 2026-04-01
```

The script drives the whole load from the command line — no need to open the UI — waiting for each stage to finish before starting the next, and aborting if any DAG run fails:

1. **Reference data (zones)** — zone lookup and shapefile, needed by staging for enrichment.
2. **Pause taxi/weather staging** so the backfill doesn't trigger them once per asset event.
3. **Backfill ingestion** for the date range (taxi + weather).
4. **Stage weather** — must run before taxi staging, since `taxi_final` reads `staging.weather` but only waits on the taxi assets.
5. **Stage taxi** — auto-discovers all missing partitions in one pass; `taxi_final` then fires automatically via assets.
6. **Register the raw catalog** (`hive_setup_raw`).

A few things about how it works:
- DAGs are created paused, and **a paused DAG won't run even when its asset fires** — the script unpauses each one before use.
- `--dag-run-conf '{"skip_repair": true}'` tells the ingestion's `update_hive` task to skip the per-run Hive repair (no Spark session per month). Staging reads raw files straight from their MinIO path (not from the Hive `raw.*` tables), so it works even though raw isn't registered yet. The raw catalog is registered once at the end.
- Backfills have their own concurrency control, separate from the DAG's `max_active_runs`. Without `--max-active-runs`, all runs fire at once and the source CDNs return HTTP 403 (rate limiting); the script passes `--max-active-runs 2` to throttle it.
- Staging is paused during ingestion so it doesn't fire once per asset event; it runs once at the end and auto-discovers all missing partitions in a single pass.

The `hive_setup_raw`, `hive_setup_staging` and `hive_setup_final` DAGs are also available on their own as operational tools to recreate the tables of a given layer when the schema changes.

## Recurring runs

After the initial load, the pipeline runs end-to-end on its own — ingestion on a schedule, staging and final layer via assets. No manual steps:

| DAG | Schedule | |
|---|---|---|
| `taxi_ingestion` | monthly | downloads Parquet files for 4 taxi types |
| `weather_ingestion` | daily | downloads weather Parquet from Open-Meteo |
| `taxi_staging` | asset-triggered | runs automatically after all 4 raw taxi assets update |
| `weather_staging` | asset-triggered | runs automatically after raw weather asset updates |
| `taxi_final` | asset-triggered | runs automatically after all 4 staging taxi assets update |

## Project structure

```
airflow/
  dags/                  DAG definitions (organized by domain: nyc, weather, zones, setup)
  staging/               staging transformations (Spark SQL + geopandas)
  final/                 final layer transformations (Spark SQL)
  utils/
    assets.py            Airflow asset definitions (single source of truth)
    hive.py              Hive Metastore table registration
    spark.py             SparkSession factory
    s3.py                MinIO helpers (boto3)
    paths.py             S3 path construction helpers
dockerfile/
  Dockerfile.airflow     custom Airflow image (Java + S3A + geopandas jars)
  Dockerfile.hive        custom Hive image (S3A jars)
  Dockerfile.spark       custom Spark image (S3A jars)
  Dockerfile.superset    custom Superset image (psycopg2 + Trino driver)
trino/
  etc/                   Trino configuration (config, jvm, catalog/hive)
superset/
  superset_config.py     Superset Python config (DB URI, secret key)
scripts/
  backfill.sh            runs the full historical backfill for a date range
  export_dashboards.sh   zips dashboard YAMLs into dashboards/exports/
dashboards/
  nyc_taxi_overview/     dashboard YAML files (versioned)
  exports/               generated ZIPs for Superset import (not versioned)
setup.sh                 initial setup script
docker-compose.yml       full infrastructure
setup/                   local files generated by setup.sh (not versioned)
  hive-lib/              JDBC driver
  hive-conf/             Hive S3A credentials
```

## Performance tuning

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

First create the Trino database connections (see [Connecting Superset to Trino](#connecting-superset-to-trino)), then:

1. Generate the ZIP:
   ```bash
   ./scripts/export_dashboards.sh
   ```
2. Go to **Dashboards → Import** and upload `dashboards/exports/nyc_taxi_overview.zip`.

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
| `nyc_taxi_overview/` | Revenue, trips, weather impact, and zone map overview |

## Connecting Superset to Trino

On first access to `localhost:3000`, log in with `admin` and the password set in `SUPERSET_ADMIN_PASSWORD`.

Go to **Settings → Database Connections → Add database** and select **Trino**. Each medallion layer is exposed as a separate database so queries and dashboards are scoped to their layer. Add three connections:

| Display Name | SQLAlchemy URI |
|---|---|
| trino raw | `trino://admin@trino:8080/hive/raw` |
| trino staging | `trino://admin@trino:8080/hive/staging` |
| trino final | `trino://admin@trino:8080/hive/final` |
