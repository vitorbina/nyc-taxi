import os
import logging
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError

from utils.constants import PARTITION_COL

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


def list_partitions(bucket: str, prefix: str) -> list[str]:
    prefix = prefix.rstrip("/") + "/"
    paginator = _get_s3_client().get_paginator("list_objects_v2")
    partitions = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            sub = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if sub.startswith(f"{PARTITION_COL}="):
                partitions.append(sub.split("=", 1)[1])
    return sorted(partitions)


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
