import os
import logging
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

# Logging configuration
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# This function uses boto3 and authenticates using credentials located in the .env file
def upload_file(filepath, bucket, key):
    
    logger.info(f"Uploading {filepath} to {bucket}/{key}...")
    
    # Getting MinIO credentials
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    
    # Creating client for MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    try:
        s3_client.upload_file(filepath, bucket, key)
        logger.info("Upload completed successfully.")
    except ClientError as e:
        logger.error(f"Erro ao fazer upload para o MinIO: {e}")
        raise e