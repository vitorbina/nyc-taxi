import requests
import boto3
import logging
import argparse

AWS_ACCESS_KEY = "minioadmin"
AWS_SECRET_KEY = "minioadmin"
ENDPOINT_URL = "http://127.0.0.1:9000"
BUCKET_NAME = "nyc-taxi-lake"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_ingestion (ano, mes):
    file_name = f"yellow_tripdata_{ano}-{mes}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{ano}-{mes}.parquet"

    object_path = f"raw/{ano}/{mes}/{file_name}"

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        endpoint_url=ENDPOINT_URL
    )

    logging.info(f"Iniciando a ingestão: {file_name}")

    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            s3_client.upload_fileobj(r.raw, BUCKET_NAME, object_path)
    except Exception as e:
        logging.error(f"Erro na pipeline: {e}")
        raise
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de dados NYC Taxi")
    parser.add_argument('--mes', type=str, required=True)
    parser.add_argument('--ano', type=str, required=True)

    args = parser.parse_args()

    run_ingestion(ano=args.ano,mes=args.mes)