# NYC Taxi Data Pipeline

A Data Engineering pipeline that ingests, cleans, and curates NYC Taxi trip records and daily weather data into a local Data Lake. Built on the **Medallion Architecture** (raw → staging → curated) with full infrastructure as code.

## Architecture

```
Sources → Raw (MinIO) → Staging (MinIO) → Curated (MinIO) → Hive Metastore → Queries
```

1. **Sources**: NYC TLC (public Parquet files) and Open-Meteo Archive API
2. **Raw**: Files ingested as-is into MinIO
3. **Staging**: PySpark transformations — type casting, filtering, column renaming
4. **Curated**: PySpark enrichment — code translation, zone joins, derived metrics
5. **Hive Metastore**: Metadata catalog — curated tables registered and queryable by name
6. **Orchestration**: Apache Airflow 3.1.7 (Local Executor)
7. **Processing**: Apache Spark 3.5.3 cluster (master + worker)

## Tech Stack

* **Apache Airflow 3.1.7**
* **Apache Spark 3.5.3**
* **Apache Hive 4.0.0** (Metastore)
* **MinIO** (S3-compatible Object Storage)
* **PostgreSQL 15** (Airflow metadata + Hive metadata)
* **PySpark 3.5.3**
* **Docker / Docker Compose**

## Prerequisites

* Docker Engine
* Docker Compose
* wget (for downloading the Hive JDBC driver)

## Setup

### 1. Clone the repo

```bash
git clone <repo-url>
cd nyc-taxi
```

### 2. Download the Hive JDBC driver

Required once before starting the infrastructure:

```bash
mkdir -p hive-lib && wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar -O hive-lib/postgresql.jar
```

### 3. Start the infrastructure

```bash
docker compose up -d
```

This starts: MinIO, PostgreSQL (Airflow), PostgreSQL (Hive), Hive Metastore, Airflow (webserver + scheduler + dag-processor), Spark (master + worker).

### 4. Get the Airflow password

```bash
docker logs nyc_airflow_webserver 2>&1 | grep Password
```

### 5. Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / (see step 4) |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| Spark Master UI | http://localhost:8081 | — |

### 6. Create the MinIO bucket

1. Open http://localhost:9001
2. Login with `minioadmin` / `minioadmin`
3. Create a bucket named exactly: `data-lake-nyc`

## Running the Pipeline

Activate the DAGs in the Airflow UI in this order:

| DAG | Schedule | Description |
|---|---|---|
| `zones_ingestion` | `@once` | Ingests taxi zone reference files |
| `zones_staging` | `@once` | Stages zone reference data |
| `taxi_ingestion` | `@monthly` | Ingests monthly taxi trip records |
| `weather_ingestion` | `@daily` | Ingests daily weather data |
| `taxi_staging` | `@monthly` | Stages and curates taxi data, updates Hive |
| `weather_staging` | `@daily` | Stages and curates weather data, updates Hive |
| `hive_setup` | `@once` | Registers curated tables in Hive Metastore |

> The staging DAGs trigger automatically via sensor when ingestion completes.
> Run `nyc_hive_setup` once after the first ingestion cycle finishes.

## Analytical Queries

Located in `/queries/`. Run locally against the Spark cluster:

```bash
source .env
python queries/covid_impact.py
python queries/market_share.py
python queries/demand_heatmap.py
python queries/weather_demand.py
```

> To use the Docker Spark cluster, set in `.env`:
> `SPARK_MASTER_URL=spark://spark-master:7077`
> `SPARK_S3A_ENDPOINT=http://minio:9000`

## Project Structure

```
dags/           Airflow DAG definitions
utils/
  staging/      PySpark staging transformations
  curated/      PySpark curated enrichment logic
  hive.py       Hive Metastore registration logic
  spark.py      SparkSession factory
  s3.py         MinIO client helpers
queries/        Analytical Spark SQL queries
hive-lib/       Hive JDBC driver (not versioned)
docker-compose.yml  Full infrastructure definition
requirements-docker.txt  Airflow container dependencies
```
