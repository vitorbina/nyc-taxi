import logging
import tempfile
import zipfile

import geopandas as gpd

from utils.s3 import download_file, upload_file, file_exists
from utils.paths import raw_key, staging_key

logger = logging.getLogger(__name__)

ZONES_ZIP_RAW_KEY = raw_key("reference/taxi_zones", file_name="taxi_zones.zip")
ZONES_GEO_STAGING_KEY = staging_key("reference/taxi_zones_geo") + "/taxi_zones_geo.parquet"


def stage_zones_geo(bucket: str) -> None:
    if not file_exists(bucket=bucket, key=ZONES_ZIP_RAW_KEY):
        raise FileNotFoundError(f"Raw file not found: {ZONES_ZIP_RAW_KEY}")

    if file_exists(bucket=bucket, key=ZONES_GEO_STAGING_KEY):
        logger.info("Zones geometry already staged, skipping")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = f"{tmpdir}/taxi_zones.zip"
        download_file(bucket=bucket, key=ZONES_ZIP_RAW_KEY, filepath=zip_path)

        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(tmpdir)

        shp_files = [f for f in __import__("os").listdir(tmpdir) if f.endswith(".shp")]
        if not shp_files:
            raise FileNotFoundError("No .shp file found inside taxi_zones.zip")

        shp_path = f"{tmpdir}/{shp_files[0]}"
        logger.info("Reading shapefile: %s", shp_path)

        gdf = gpd.read_file(shp_path)

        if gdf.crs and gdf.crs.to_epsg() != 4326:
            logger.info("Converting CRS from %s to WGS84", gdf.crs.to_epsg())
            gdf = gdf.to_crs(epsg=4326)

        gdf["geometry_wkt"] = gdf["geometry"].apply(lambda g: g.wkt if g else None)

        df = gdf[["LocationID", "borough", "zone", "Shape_Area", "geometry_wkt"]].copy()
        df.columns = ["location_id", "borough", "zone", "shape_area", "geometry_wkt"]
        df["location_id"] = df["location_id"].astype(int)

        parquet_path = f"{tmpdir}/taxi_zones_geo.parquet"
        df.to_parquet(parquet_path, index=False)
        logger.info("Wrote %d zones to parquet", len(df))

        upload_file(filepath=parquet_path, bucket=bucket, key=ZONES_GEO_STAGING_KEY)
        logger.info("Uploaded to %s/%s", bucket, ZONES_GEO_STAGING_KEY)
