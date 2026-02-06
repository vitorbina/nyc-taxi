# NYC Taxi Data Pipeline

This is a Data Engineering pipeline designed to extract historical NYC Taxi Trip records (TLC Data), process them, and store them in a local Data Lake using **MinIO**.

## Architecture

1.  **Source**: NYC TLC Data (Public Parquet/CSV files).
2.  **Orchestration**: Apache Airflow 2.10 (Local Executor).
3.  **Storage (Bronze Layer)**: MinIO (S3 Compatible Object Storage).

## Tech Stack

* **Python 3.x**
* **Apache Airflow**
* **MinIO / S3**
* **Pandas & PyArrow**

##Setup & Installation

To set up the local environment (install dependencies and configure Airflow Home), simply run the setup script:

```bash
# 1. Grant execution permission
chmod +x setup_env.sh

# 2. Run automatic setup
./setup_env.sh
```

## How to Run

Once the environment is configured, start Airflow:

```bash
# Terminal 1: Scheduler
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler

# Terminal 2: Webserver
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver
```

Access the UI at: `http://localhost:8080`

## Project Structure

* `/dags`: Airflow DAG definitions (Orchestration logic).
* `/scripts`: Pure Python scripts for data ingestion (ETL logic).
* `/airflow`: Local Airflow metadata and logs (ignored by Git).
