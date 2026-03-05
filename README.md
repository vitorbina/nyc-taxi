# NYC Taxi Data Pipeline

This is a Data Engineering pipeline designed to extract historical NYC Taxi Trip records (TLC Data), process them, and store them in a local Data Lake using **MinIO**.

## Architecture

1. **Source**: NYC TLC Data (Public Parquet).
2. **Ingestion**: Modular Python logic using requests and tempfile to ensure data integrity and disk cleanup.
3. **Orchestration**: Apache Airflow 3.1.7 (Local Executor).
4. **Storage**: MinIO (S3 Compatible Object Storage).

## Tech Stack

* **Python 3.13.11**
* **Apache Airflow 3.1.7**
* **MinIO / S3**

## Setup & Installation

### 1. Prerequisites (System Level)

Before cloning the repo, ensure your operating system has these core tools installed:

* **Python - 3.13.11**
* **Docker Engine - 29.2.1**
* **Docker Compose - 5.1.0**

### 2. Environment Setup (Using Conda)

We use Conda for environment management to ensure clean dependency isolation.

```bash
# 1. Create the environment with the specific Python version (if not created yet)
conda create --name your_project_name python=3.13.11 -y

# 2. Activate the environment
conda activate your_project_name

# 3. Install the project dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

## How to Run

> **Note:** You will need at least **two terminal tabs** open for this.

#### Infrastructure & Orchestration

```bash
# 1. Load environment variables
source .env

# 2. Start MinIO in background
docker compose up -d minio

# 3. Start Airflow (This will stay active showing logs)
airflow standalone
```

#### UI Access & Monitoring

Once the services are running, open your browser to access the web interfaces:

* **Airflow UI**: http://localhost:8080
* **MinIO Console**: http://localhost:9001

## MinIO Bucket Setup (Required)
Before running the pipeline, you must create the destination bucket:

1. Access the MinIO Console using the link above.

2. Login using the credentials from your .env file.

3. Navigate to Buckets on the left menu and click Create Bucket.

4. Name the bucket exactly: data-lake-nyc

5. Click Create Bucket. Your local Data Lake is ready!

## Project Structure

* `/dags`: Airflow DAG definitions (Orchestration logic).
* `/scripts`: Pure Python scripts for data ingestion (ETL logic).
* `/airflow`: Local Airflow metadata and logs (ignored by Git).
* `docker-compose.yaml`: Infrastructure configuration for MinIO.
* `requirements.txt`: Python project dependencies.
* `.env`: Local environment variables and credentials (ignored by Git).