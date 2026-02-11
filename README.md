# NYC Taxi Data Pipeline

This is a Data Engineering pipeline designed to extract historical NYC Taxi Trip records (TLC Data), process them, and store them in a local Data Lake using **MinIO**.

## Architecture

1.  **Source**: NYC TLC Data (Public Parquet).
2.  **Orchestration**: Apache Airflow 3.1.7 (Local Executor).
3.  **Storage**: MinIO (S3 Compatible Object Storage).

## Tech Stack

* **Python 3.13.11**
* **Apache Airflow 3.1.7**
* **MinIO / S3**
* **Pandas**

## Setup & Installation

To set up the local environment (install dependencies and configure Airflow Home), simply run the setup script:

```bash
# 1. Grant execution permission
chmod +x setup_env.sh

# 2. Run automatic setup
./setup_env.sh
```

## How to Run

Once the environment is configured, start Airflow:

\`\`\`bash
# 1. Export the home variable (MANDATORY step)
export AIRFLOW_HOME=\$(pwd)/airflow

# 2. Run everything (Scheduler + Webserver + Triggerer)
airflow standalone
\`\`\`

Access the UI at: `http://localhost:8080`

## Project Structure

* `/dags`: Airflow DAG definitions (Orchestration logic).
* `/scripts`: Pure Python scripts for data ingestion (ETL logic).
* `/airflow`: Local Airflow metadata and logs (ignored by Git).
