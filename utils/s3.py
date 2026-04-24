import os
import logging
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )


def file_exists(bucket: str, key: str) -> bool:
    try:
        _get_s3_client().head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def folder_exists(bucket: str, prefix: str) -> bool:
    prefix = prefix.rstrip("/") + "/"
    response = _get_s3_client().list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get("KeyCount", 0) > 0


def download_file(bucket: str, key: str, filepath: str) -> None:
    logger.info("Downloading %s/%s to %s...", bucket, key, filepath)
    try:
        _get_s3_client().download_file(bucket, key, filepath)
        logger.info("Download completed successfully.")
    except ClientError as e:
        logger.error("Download from MinIO failed: %s", e)
        raise


def upload_file(filepath: str, bucket: str, key: str) -> None:
    logger.info("Uploading %s to %s/%s...", filepath, bucket, key)
    try:
        _get_s3_client().upload_file(filepath, bucket, key)
        logger.info("Upload completed successfully.")
    except ClientError as e:
        logger.error("Upload to MinIO failed: %s", e)
        raise
