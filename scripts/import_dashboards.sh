#!/usr/bin/env bash
# Import dashboards into Superset via the REST API with overwrite=true.
# The `superset import-dashboards` CLI does NOT overwrite existing charts/datasets,
# so chart-level changes never take effect. The API import (same path the UI uses)
# does, when overwrite=true is passed.
set -e

SUPERSET_URL="${SUPERSET_URL:-http://localhost:3000}"
ADMIN_USER="${SUPERSET_ADMIN_USER:-admin}"
ADMIN_PASS="${SUPERSET_ADMIN_PASSWORD:-admin}"

DASHBOARDS_DIR="$(cd "$(dirname "$0")/../dashboards" && pwd)"
ZIP_PATH="$DASHBOARDS_DIR/exports/nyc_taxi_overview.zip"
COOKIE_JAR="$(mktemp)"
trap 'rm -f "$COOKIE_JAR"' EXIT

if [ ! -f "$ZIP_PATH" ]; then
    echo "Zip not found: $ZIP_PATH — run ./scripts/export_dashboards.sh first." >&2
    exit 1
fi

echo "Logging in as $ADMIN_USER..."
ACCESS_TOKEN=$(curl -s -c "$COOKIE_JAR" -X POST "$SUPERSET_URL/api/v1/security/login" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASS\",\"provider\":\"db\",\"refresh\":true}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

echo "Fetching CSRF token..."
CSRF_TOKEN=$(curl -s -b "$COOKIE_JAR" -c "$COOKIE_JAR" "$SUPERSET_URL/api/v1/security/csrf_token/" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])")

echo "Importing $ZIP_PATH (overwrite=true)..."
curl -s -b "$COOKIE_JAR" -X POST "$SUPERSET_URL/api/v1/dashboard/import/" \
    -H "Authorization: Bearer $ACCESS_TOKEN" \
    -H "X-CSRFToken: $CSRF_TOKEN" \
    -H "Referer: $SUPERSET_URL" \
    -F "formData=@$ZIP_PATH;type=application/zip" \
    -F "overwrite=true"
echo
echo "Done."
