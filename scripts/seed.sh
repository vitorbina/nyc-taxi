#!/usr/bin/env bash
#
# Seed the lake with a short, fixed window of data.
#
# Runs a 3-month backfill so anyone who clones the repo can explore the
# dashboards without committing to the full historical load. Reuses backfill.sh
# under the hood. Run it after the stack is up (docker compose up -d).
#
# Usage:
#   ./scripts/seed.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

FROM_DATE="2025-01-01"
TO_DATE="2025-04-01"

echo ">>> Seeding 3 months of data ($FROM_DATE -> $TO_DATE)..."
"$SCRIPT_DIR/backfill.sh" "$FROM_DATE" "$TO_DATE"
echo ">>> Seed complete. Open Superset at http://localhost:3000."
