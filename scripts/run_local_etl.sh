#!/usr/bin/env bash
set -euo pipefail

SOURCE_DB="${1:-../propertyguru-sg-weekly-snapshots-prod-20260403-125756/data/state/propertyguru.sqlite}"
OUTPUT_DB="${2:-data/analytics/listing_intel.sqlite}"

python -m sg_listing_intel.etl --source "$SOURCE_DB" --output "$OUTPUT_DB"
