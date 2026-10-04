#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
CONTRACT="$REPO_ROOT/docs/specs/SILVER_GOLD_DATA_MODEL.md"
ML_CONTRACT="$REPO_ROOT/docs/specs/ML_DATA_MODEL.md"
TASK="$REPO_ROOT/docs/task/tasks/CON-03.md"
INDEX="$REPO_ROOT/docs/task/tasks/README.md"

fail() {
  printf '%s\n' "Silver/Gold/ML data model check failed: $1" >&2
  exit 1
}

require_count() {
  file=$1
  pattern=$2
  expected=$3
  actual=$(rg -c -F -- "$pattern" "$file" || true)
  [ "$actual" = "$expected" ] || fail "expected $expected occurrence(s), found ${actual:-0}: $pattern"
}

[ -f "$CONTRACT" ] || fail "missing docs/specs/SILVER_GOLD_DATA_MODEL.md"
[ -f "$ML_CONTRACT" ] || fail "missing docs/specs/ML_DATA_MODEL.md"
[ -f "$TASK" ] || fail "missing docs/task/tasks/CON-03.md"
[ -f "$INDEX" ] || fail "missing docs/task/tasks/README.md"

for pattern in \
  'contract_id: "CON-03"' \
  'contract_version: "2.0"' \
  'status: "Active"' \
  'ml_schema_version: "1.0"' \
  '[ML logical data model](./ML_DATA_MODEL.md)' \
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

for pattern in \
  'contract_id: "CON-03-ML"' \
  'contract_version: "1.0"' \
  'status: "Active"' \
  '`ml.dataset_manifest`' \
  '`ml.mainshock_candidate_snapshot`' \
  '`ml.sequence_candidate_snapshot`' \
  '`ml.experiment_run`' \
  '`ml.sequence_membership`' \
  '`ml.sequence_summary`' \
  '(dataset_id, mainshock_event_id, candidate_event_id)' \
  '(experiment_run_id, mainshock_event_id, candidate_event_id)' \
  '(experiment_run_id, mainshock_event_id)' \
  '`gold_snapshot_id`' \
  '`dataset_id`' \
  '`experiment_run_id`' \
  '`x_scaled`' \
  '`time_scaled`' \
  '`mc_config_json`' \
  '`membership_probability`' \
  '`event_role_candidate`' \
  '`multi_sequence_status`' \
  '`failure_reason_code`' \
  '`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`, `HDBSCAN_ADAPTIVE`' \
  '`BUILDING`, `VALIDATED`, `EXPORTED`, `REJECTED`' \
  '`TRAINING_EXTERNAL`, `RESULT_READY`, `IMPORT_VALIDATING`, `CANDIDATE`, `APPROVED`, `REJECTED`' \
  '`DS_IDENTITY_CONFLICT`' \
  '`EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`' \
  '`IMP_CHECKSUM_MISMATCH`' \
  '`IMP_EXPERIMENT_RUN_REUSED`' \
  '`METRIC_FIT_FAILED`' \
  'không tự chuyển' \
  'không phải prediction hay causal label' \
  'Không thêm cluster, probability hoặc nhãn sequence vào Gold core'
do
  rg -F -- "$pattern" "$ML_CONTRACT" >/dev/null || fail "missing required ML model rule: $pattern"
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

require_count "$CONTRACT" '| `magnitude_type` | `properties.magType` | Magnitude type/code JMA |' 1
require_count "$CONTRACT" "WHERE event_time_utc >= TIMESTAMP '<start_utc>'" 1
require_count "$ML_CONTRACT" '| `dataset_id` | string | Không | Primary logical key, theo mục 3.1 |' 1
require_count "$ML_CONTRACT" '| `experiment_run_id` | string | Không | Primary logical key, không reuse |' 1

if rg -q '(^|[^A-Z])(TBD|TODO|FIXME)([^A-Z]|$)' "$CONTRACT" "$ML_CONTRACT"; then
  fail 'contract contains unresolved placeholder'
fi

rg -F -- '[ML logical data model](../../specs/ML_DATA_MODEL.md)' "$TASK" >/dev/null || \
  fail 'CON-03 task does not link the ML contract'
rg -F -- 'status: "Done"' "$TASK" >/dev/null || \
  fail 'CON-03 task metadata is not Done'
rg -F -- '| [CON-03](./CON-03.md) - Thiết kế mô hình dữ liệu Silver, Gold và ML | 2 | Data model contract | Core | P0 | 6h | Done |' "$INDEX" >/dev/null || \
  fail 'task index does not mark CON-03 Done'

printf '%s\n' 'CON-03 Silver/Gold/ML data model contract check passed.'
