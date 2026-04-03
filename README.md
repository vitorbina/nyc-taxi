# NYC Taxi Data Pipeline

A Data Engineering pipeline that ingests, processes, and serves NYC Taxi trip records and daily weather data into a local Data Lake. Built on the **Medallion Architecture** (raw → staging → final) with full infrastructure as code.

## Architecture

```
Sources → Raw (MinIO) → Staging (MinIO) → Hive Metastore
```

1. **Sources**: NYC TLC (public Parquet files) and Open-Meteo Archive API
2. **Raw**: Files ingested as-is into MinIO
3. **Staging**: PySpark (SQL) transformations — type casting, filtering, column renaming, zone joins, code translations
4. **Hive Metastore**: Metadata catalog — staging tables registered and queryable by name
5. **Orchestration**: Apache Airflow 3.1.7 (Local Executor)
6. **Processing**: Apache Spark 3.5.3 cluster (master + worker)

## Tech Stack

| Component | Version |
|---|---|
| Apache Airflow | 3.1.7 |
| Apache Spark | 3.5.3 |
| Apache Hive Metastore | 4.0.0 |
| MinIO | latest |
| PostgreSQL | 15 |
| PySpark | 3.5.3 |
| Docker / Docker Compose | — |

## Prerequisites

* Docker Engine
* Docker Compose
* `wget` (for downloading drivers)

---

## Setup

### 1. Clone the repo

```bash
git clone <repo-url>
cd nyc-taxi
```

### 2. Run the setup script

Downloads the Hive JDBC driver, creates the Hive S3A config, and creates the MinIO bucket automatically:

```bash
./setup.sh
```

### 3. Build and start the infrastructure

```bash
docker compose up -d
```

On first run this will build two custom Docker images (takes a few minutes — downloads S3A jars from Maven):
- **Dockerfile.airflow** — Airflow with Java and S3A jars so PySpark can read/write MinIO
- **Dockerfile.hive** — Hive Metastore with S3A jars so it can scan MinIO partitions

> You may see warnings like `The "PYTHONPATH" variable is not set` — these are harmless.

Wait for all containers to be healthy before proceeding:

```bash
docker compose ps
```

### 4. Get the Airflow admin password

The password is auto-generated on first start:

```bash
docker exec nyc_airflow_webserver cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

> If you restart the stack without deleting volumes, the password stays the same.
> If you do a full teardown (`docker compose down -v`), a new password is generated on next start.

### 5. Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / (from step 4) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8081 | — |

---

## Running the Pipeline

Go to the Airflow UI (http://localhost:8080) and activate the DAGs in the following order. Each DAG must complete before enabling the next group.

### Step 1 — Reference data (run once)

| DAG | Description |
|---|---|
| `zones_ingestion` | Downloads taxi zone lookup CSV into raw layer |
| `zones_staging` | Cleans and stages zone data — **wait for zones_ingestion to finish** |

### Step 2 — Setup Hive (run once, after step 1)

| DAG | Description |
|---|---|
| `hive_setup` | Registers all staging tables in the Hive Metastore |

> Run this once after `zones_staging` completes. Re-run it if you ever wipe the Hive Metastore database.

### Step 3 — Ongoing ingestion (runs on schedule)

| DAG | Schedule | Description |
|---|---|---|
| `taxi_ingestion` | `@monthly` | Downloads monthly taxi Parquet files into raw layer |
| `weather_ingestion` | `@daily` | Downloads daily weather JSON into raw layer |
| `taxi_staging` | `@monthly` | Triggered automatically after `taxi_ingestion` — cleans, enriches, and updates Hive |
| `weather_staging` | `@daily` | Triggered automatically after `weather_ingestion` — cleans and updates Hive |

> `taxi_staging` and `weather_staging` use sensors to wait for ingestion to finish before running — you don't need to trigger them manually.

---

## Project Structure

```
dags/                    Airflow DAG definitions
utils/
  staging/               PySpark staging transformations (SQL-based)
  hive.py                Hive Metastore registration logic
  spark.py               SparkSession factory
  s3.py                  MinIO client helpers
setup.sh                 One-time setup script (run before first docker compose up)
Dockerfile.airflow       Custom Airflow image (Java + S3A jars)
Dockerfile.hive          Custom Hive Metastore image (S3A jars)
docker-compose.yml       Full infrastructure definition
requirements.txt         Python dependencies
hive-lib/                Hive JDBC driver — not versioned, created by setup.sh
hive-conf/               Hive S3A credentials — not versioned, created by setup.sh
```
