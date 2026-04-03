# NYC Taxi Data Pipeline

Data engineering pipeline that ingests, processes, and serves NYC taxi trip records and daily weather data into a local Data Lake.

Medallion architecture (raw → staging) with Airflow orchestrating, Spark processing via SQL, MinIO as storage, and Hive Metastore as the catalog.

## Architecture

```
Sources (NYC TLC, Open-Meteo)
  ↓
Raw (MinIO) ── raw files, no transformation
  ↓
Staging (MinIO) ── cleaning, type casting, joins, code translations (Spark SQL)
  ↓
Hive Metastore ── registered tables, queryable by name
```

## Stack

| | |
|---|---|
| Airflow | 3.1.7 |
| Spark | 3.5.3 |
| Hive Metastore | 4.0.0 |
| MinIO | latest |
| PostgreSQL | 15 |
| Docker Compose | — |

## Prerequisites

- Docker and Docker Compose
- wget

## Getting started

```bash
git clone <repo-url>
cd nyc-taxi

# initial setup (JDBC driver, Hive config, MinIO bucket)
./setup.sh

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
| MinIO | localhost:9001 | minioadmin / minioadmin |
| Spark | localhost:8081 | — |

## Pipeline

Enable DAGs in the Airflow UI in this order:

**1. Reference data (run once)**

- `zones_ingestion` — downloads zone lookup CSV from NYC TLC
- `zones_staging` — cleans and converts to Parquet (wait for ingestion to finish)

**2. Hive (run once, after step 1)**

- `hive_setup` — registers tables in the metastore

**3. Recurring ingestion**

| DAG | Schedule | |
|---|---|---|
| `taxi_ingestion` | monthly | downloads Parquet files for 4 taxi types |
| `weather_ingestion` | daily | downloads weather JSON from Open-Meteo |
| `taxi_staging` | monthly | automatic staging after ingestion |
| `weather_staging` | daily | automatic staging after ingestion |

Staging DAGs have sensors — they trigger on their own when ingestion finishes.

## Project structure

```
dags/                    DAG definitions
utils/
  staging/               staging transformations (Spark SQL)
  hive.py                Hive Metastore table registration
  spark.py               SparkSession factory
  s3.py                  MinIO helpers (boto3)
setup.sh                 initial setup script
Dockerfile.airflow       custom Airflow image (Java + S3A jars)
Dockerfile.hive          custom Hive image (S3A jars)
docker-compose.yml       full infrastructure
requirements.txt         Python dependencies
hive-lib/                JDBC driver (created by setup.sh)
hive-conf/               Hive S3A config (created by setup.sh)
```
