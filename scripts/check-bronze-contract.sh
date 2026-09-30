#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CONTRACT="$REPO_ROOT/docs/specs/BRONZE_STORAGE_CONTRACT.md"

fail() {
  printf '%s\n' "Bronze contract check failed: $1" >&2
  exit 1
}

[ -f "$CONTRACT" ] || fail "missing docs/specs/BRONZE_STORAGE_CONTRACT.md"

for pattern in \
  'contract_id: "CON-02"' \
  'contract_version: "1.0"' \
  'bronze/usgs/ingest_date=YYYY-MM-DD/run_id=<run-id>/attempt=<nn>/' \
  'bronze/jma/year=YYYY/catalog_release=<release-slug>/' \
  'bronze/_staging/' \
  'bronze/_quarantine/' \
  'bronze_status=BronzeReady' \
  'AMBIGUOUS_OVERWRITE' \
  'raw_object_uri' \
  'sha256' \
  'retrieved_at_utc' \
  'catalog_release' \
  'raw_readback_verified' \
  'source_structure_valid' \
  'retry_of_manifest_uri'
do
  rg -F -- "$pattern" "$CONTRACT" >/dev/null || fail "missing required contract rule: $pattern"
done

rg -F -- 'BronzeReady' "$CONTRACT" >/dev/null || fail "BronzeReady state is not documented"
rg -F -- 'Rejected' "$CONTRACT" >/dev/null || fail "Rejected state is not documented"
rg -F -- 'manifest.json' "$CONTRACT" >/dev/null || fail "manifest object is not documented"

printf '%s\n' 'CON-02 Bronze storage contract check passed.'
