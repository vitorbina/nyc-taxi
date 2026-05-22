import logging
import os
import shutil
import tempfile

import requests

from utils.s3 import upload_file
from utils.paths import raw_key

logger = logging.getLogger(__name__)

TLC_ZONE_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/misc"


def _download_zone_file(file_name: str, local_path: str) -> None:
    url = f"{TLC_ZONE_DATA_URL}/{file_name}"
    logger.info("Downloading %s...", url)

    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)

    logger.info("File saved at: %s", local_path)


def _upload_zone_file(local_path: str, file_name: str, bucket: str) -> None:
    folder_name = os.path.splitext(file_name)[0]
    key = raw_key(f"reference/{folder_name}", file_name=file_name)
    logger.info("Uploading to %s/%s", bucket, key)
    upload_file(filepath=local_path, bucket=bucket, key=key)


def ingest_zone_data(file_name: str, bucket: str) -> None:
    tmpdir = tempfile.mkdtemp()
    try:
        local_path = os.path.join(tmpdir, file_name)
        _download_zone_file(file_name, local_path)
        _upload_zone_file(local_path, file_name, bucket)
    finally:
        shutil.rmtree(tmpdir)
