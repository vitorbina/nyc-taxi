import os
import logging
import requests
import tempfile
import shutil
from utils.s3 import upload_file

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/misc"


def download_zone_file(file_name):
    tmpfolder = tempfile.mkdtemp()
    local_path = os.path.join(tmpfolder, file_name)

    url = f"{BASE_URL}/{file_name}"
    logger.info(f"Downloading {url}...")

    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, 'wb') as f:
        f.write(response.content)

    logger.info(f"File saved at: {local_path}")
    return local_path


def upload_zone_file(local_path, file_name, bucket):
    folder_name = os.path.splitext(file_name)[0]
    key = f"raw/reference/{folder_name}/{file_name}"

    logger.info(f"Uploading to {bucket}/{key}")
    upload_file(filepath=local_path, bucket=bucket, key=key)

    tmpfolder = os.path.dirname(local_path)
    shutil.rmtree(tmpfolder)
    logger.info(f"Temporary folder {tmpfolder} removed.")


def ingest_zone_data(file_name, bucket):
    local_path = download_zone_file(file_name)
    upload_zone_file(local_path, file_name, bucket)
