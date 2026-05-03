#!/usr/bin/env bash
set -euo pipefail

SOURCE_DB="${SOURCE_DB:-../propertyguru-sg-weekly-snapshots-prod-20260403-125756/data/state/propertyguru.sqlite}"
ANALYTICS_DB="${ANALYTICS_DB:-data/analytics/listing_intel.sqlite}"
PYTHON_BIN="${PYTHON:-}"

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x ".venv/bin/python" ]]; then
    PYTHON_BIN=".venv/bin/python"
  else
    PYTHON_BIN="python3"
  fi
fi

"$PYTHON_BIN" -m sg_listing_intel.etl --source "$SOURCE_DB" --output "$ANALYTICS_DB"
"$PYTHON_BIN" scripts/export_static_data.py --db "$ANALYTICS_DB" --out docs/assets

if [[ "${DASHBOARD_AUTO_PUBLISH:-0}" == "1" ]]; then
  git add docs/assets/dashboard-data.json docs/assets/manifest.json
  if git diff --cached --quiet; then
    echo "No dashboard static data changes to publish."
  else
    git commit -m "Refresh dashboard data from latest PG snapshot"
    git push
  fi
fi
