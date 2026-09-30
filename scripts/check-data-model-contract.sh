#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CONTRACT="$REPO_ROOT/docs/specs/SILVER_GOLD_DATA_MODEL.md"

fail() {
  printf '%s\n' "Silver/Gold data model check failed: $1" >&2
  exit 1
}

require_count() {
  pattern=$1
  expected=$2
  actual=$(rg -c -F -- "$pattern" "$CONTRACT" || true)
  [ "$actual" = "$expected" ] || fail "expected $expected occurrence(s), found ${actual:-0}: $pattern"
}

[ -f "$CONTRACT" ] || fail "missing docs/specs/SILVER_GOLD_DATA_MODEL.md"

for pattern in \
  'contract_id: "CON-03"' \
  'contract_version: "1.0"' \
  '`silver.source_observation`' \
  '`silver.reject_record`' \
  '`silver.source_link`' \
  '`silver.canonical_membership`' \
  '`gold.event_current`' \
  '`gold.earthquake_event_current`' \
  '`gold.event_source_bridge`' \
  '`gold.event_daily`' \
  '`gold.publication_status`' \
  '`source_observation_id`' \
  '`source_record_key`' \
  '`source_revision_key`' \
  '`canonical_event_id`' \
  '`event_time_utc`' \
  '`event_time_jst`' \
  '`raw_object_uri`' \
  '`raw_sha256`' \
  '`quality_flags`' \
  '`magnitude_band_code`' \
  '`depth_band_code`' \
  '`source_coverage_code`' \
  '`UNKNOWN`' \
  '`OFFSHORE`' \
  'COUNT(DISTINCT canonical_event_id)' \
  'không đổi magnitude/depth thiếu thành `0`'
do
  rg -F -- "$pattern" "$CONTRACT" >/dev/null || fail "missing required model rule: $pattern"
done

for boundary in \
  '`LT_3`' \
  '`M3_TO_LT4`' \
  '`M4_TO_LT5`' \
  '`M5_TO_LT6`' \
  '`M6_TO_LT7`' \
  '`GE_7`' \
  '`NEGATIVE`' \
  '`SHALLOW`' \
  '`INTERMEDIATE`' \
  '`DEEP`'
do
  rg -F -- "$boundary" "$CONTRACT" >/dev/null || fail "missing band boundary: $boundary"
done

require_count '| `magnitude_type` | `properties.magType` | Magnitude type/code JMA |' 1
require_count "WHERE event_time_utc >= TIMESTAMP '<start_utc>'" 1

printf '%s\n' 'CON-03 Silver/Gold data model contract check passed.'
