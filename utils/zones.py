import os
import logging
import requests
import tempfile
import shutil
from utils.s3 import upload_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/misc"


def download_zone_file(file_name: str, local_path: str) -> None:
    url = f"{BASE_URL}/{file_name}"
    logger.info(f"Downloading {url}...")

    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)

    logger.info(f"File saved at: {local_path}")


def upload_zone_file(local_path: str, file_name: str, bucket: str) -> None:
    folder_name = os.path.splitext(file_name)[0]
    key = f"raw/reference/{folder_name}/{file_name}"

    logger.info(f"Uploading to {bucket}/{key}")
    upload_file(filepath=local_path, bucket=bucket, key=key)


def ingest_zone_data(file_name: str, bucket: str) -> None:
    tmpfolder = tempfile.mkdtemp()
    try:
        local_path = os.path.join(tmpfolder, file_name)
        download_zone_file(file_name, local_path)
        upload_zone_file(local_path, file_name, bucket)
    finally:
        shutil.rmtree(tmpfolder)
