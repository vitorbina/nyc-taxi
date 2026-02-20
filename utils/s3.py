import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def upload_file(filepath, bucket, key):
    
    logging.info(f"Uploading {filepath} to {bucket}/{key}...")
    
    hook = S3Hook(aws_conn_id='minio_conn')
    
    hook.load_file(
        filename=filepath,
        bucket_name=bucket,
        key=key,
        replace=True
    )
    
    logging.info("Upload completed successfully.")