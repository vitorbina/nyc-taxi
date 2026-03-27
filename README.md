# NYC Taxi Data Pipeline

A Data Engineering pipeline that ingests, cleans, and curates NYC Taxi trip records and daily weather data into a local Data Lake using **MinIO**, **Apache Airflow**, and **PySpark**. Built on the Medallion Architecture (raw → staging → curated).

## Architecture

1. **Sources**: NYC TLC (public Parquet files) and Open-Meteo Archive API.
2. **Raw**: Ingestion via `requests` and `tempfile`, stored as-is in MinIO.
3. **Staging**: PySpark transformations — type casting, filtering, and column renaming.
4. **Curated**: PySpark enrichment — ID translation, zone joins, and derived metrics.
5. **Orchestration**: Apache Airflow 3.1.7 (Local Executor).
6. **Storage**: MinIO (S3-compatible Object Storage).

## Tech Stack

* **Python 3.13.11**
* **Apache Airflow 3.1.7**
* **PySpark 3.5.3**
* **MinIO / S3**
* **Open-Meteo API**
* **Docker / Docker Compose**

## Setup & Installation

### 1. Prerequisites (System Level)

Before cloning the repo, ensure your operating system has these core tools installed:

* **Python 3.13.11**
* **Docker Engine 29.2.1**
* **Docker Compose 5.1.0**

### 2. Environment Setup (Using Conda)

```bash
# 1. Create the environment with the specific Python version (if not created yet)
conda create --name your_project_name python=3.13.11 -y

# 2. Activate the environment
conda activate your_project_name

# 3. Install the project dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Copy the example file and fill in your credentials:

```bash
cp .env.example .env
```

Edit `.env` with your values:

| Variable | Description |
|---|---|
| `MINIO_ROOT_USER` | MinIO username |
| `MINIO_ROOT_PASSWORD` | MinIO password |
| `MINIO_ENDPOINT` | MinIO API URL (e.g. `http://localhost:9000`) |

## How to Run

```bash
source .env
docker compose up -d minio
airflow standalone
```

#### UI Access

Once the services are running, open your browser:

* **Airflow UI**: http://localhost:8080
* **MinIO Console**: http://localhost:9001

## MinIO Bucket Setup (Required)

Before running the pipelines, create the destination bucket:

1. Open the MinIO Console at http://localhost:9001
2. Login with the credentials from your `.env` file
3. Navigate to **Buckets** on the left menu and click **Create Bucket**
4. Name the bucket exactly: `data-lake-nyc`
5. Click **Create Bucket**


## Project Structure

* `/dags`: Airflow DAG definitions for each pipeline layer.
* `/utils`: Python modules organized by layer:
  * `/utils/staging`: PySpark transformations for the staging layer.
  * `/utils/curated`: PySpark enrichment logic for the curated layer.
* `/queries`: Analytical Spark SQL queries on the curated layer.
* `docker-compose.yml`: Infrastructure configuration for MinIO.
* `requirements.txt`: Python project dependencies.
* `.env`: Local environment variables and credentials (ignored by Git).
