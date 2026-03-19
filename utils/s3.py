import os
import hashlib
import logging
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def _get_s3_client():
    return boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )


def compute_file_hash(filepath):
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return sha256.hexdigest()


def file_hash_matches(filepath, bucket, key):
    local_hash = compute_file_hash(filepath)
    try:
        response = _get_s3_client().head_object(Bucket=bucket, Key=key)
        stored_hash = response.get('Metadata', {}).get('content-hash')
        return stored_hash == local_hash
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise


def upload_file(filepath, bucket, key):
    logger.info(f"Uploading {filepath} to {bucket}/{key}...")
    content_hash = compute_file_hash(filepath)
    try:
        _get_s3_client().upload_file(
            filepath, bucket, key,
            ExtraArgs={'Metadata': {'content-hash': content_hash}}
        )
        logger.info("Upload completed successfully.")
    except ClientError as e:
        logger.error(f"Upload to MinIO failed: {e}")
        raise
