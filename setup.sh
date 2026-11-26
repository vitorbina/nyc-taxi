#!/usr/bin/env bash
# One-time local prep. Everything the stack needs at runtime is in the repo or created
# by docker compose itself (Hive JDBC driver and core-site.xml are built into the image,
# the minio-init service creates the bucket), so a server (Dokploy) needs no setup step.
set -e

if [ ! -f .env ]; then
    echo "ERROR: .env file not found. Copy .env.example to .env and fill in the values."
    exit 1
fi

echo "==> Starting MinIO and creating the bucket..."
docker compose up -d minio minio-init
# docker wait (not docker compose wait) also returns for a container that already exited
docker wait nyc_minio_init > /dev/null
echo "    Done"

echo ""
echo "Setup complete. Run 'docker compose up -d' to start the full infrastructure."
