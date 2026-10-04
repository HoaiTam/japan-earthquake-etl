#!/usr/bin/env bash

set -euo pipefail

dag_id=usg_04_usgs_ingest
run_id="usg06-live-$(date -u +%Y%m%dT%H%M%SZ)-${HOSTNAME:-container}"
safe_run_id=${run_id//[^A-Za-z0-9._~-]/-}
run_root="/opt/pipeline/staging/usgs/${safe_run_id}"
runner=/opt/pipeline/bin/usgs-ingest-runner
max_attempts=600
attempt=1
conf='{"is_backfill":true,"window_start_utc":"2023-01-01T00:00:00Z","window_end_utc":"2023-01-04T00:00:00Z"}'

python - <<'PY'
import json
import subprocess

errors = json.loads(
    subprocess.check_output(
        ["airflow", "dags", "list-import-errors", "--output", "json"],
        text=True,
    )
)
if errors:
    raise SystemExit(f"Airflow DAG import errors detected: {errors}")

dags = json.loads(
    subprocess.check_output(
        ["airflow", "dags", "list", "--output", "json"],
        text=True,
    )
)
if not any(item.get("dag_id") == "usg_04_usgs_ingest" for item in dags):
    raise SystemExit("usg_04_usgs_ingest is missing from the serialized DAG list")
PY

restore_dag_pause() {
    airflow dags pause "$dag_id" --yes >/dev/null 2>&1 || true
}
trap restore_dag_pause EXIT
airflow dags unpause "$dag_id" --yes >/dev/null

airflow dags trigger "$dag_id" --run-id "$run_id" --conf "$conf" >/dev/null

while [ "$attempt" -le "$max_attempts" ]; do
    state=$(airflow dags state "$dag_id" "$run_id" 2>/dev/null | tail -n 1 | tr -d '\r')
    # Airflow 3.3 prints "<state>, <run_conf>"; only the first token is state.
    state=${state%%,*}
    case "$state" in
        success) break ;;
        failed)
            printf 'USG-06 live smoke DAG failed: dag_id=%s run_id=%s\n' \
                "$dag_id" "$run_id" >&2
            exit 1
            ;;
    esac
    sleep 2
    attempt=$((attempt + 1))
done

if [ "$attempt" -gt "$max_attempts" ]; then
    printf 'USG-06 live smoke DAG timed out: dag_id=%s run_id=%s\n' \
        "$dag_id" "$run_id" >&2
    exit 1
fi

summary_file="$run_root/run_summary.json"
upload_context="$run_root/upload-input.json"
verify_context="$run_root/verify-input.json"
for required_file in "$summary_file" "$upload_context" "$verify_context"; do
    if [ ! -r "$required_file" ]; then
        printf 'USG-06 live smoke output is missing: %s\n' "$required_file" >&2
        exit 1
    fi
done

rerun_upload=$($runner --phase upload --context-file "$upload_context")
rerun_verify=$($runner --phase verify --context-file "$verify_context")

python - "$summary_file" "$rerun_upload" "$rerun_verify" <<'PY'
import json
from pathlib import Path
import sys

summary = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
upload = json.loads(sys.argv[2])
verify = json.loads(sys.argv[3])

if summary.get("status") != "BronzeReady":
    raise SystemExit("run summary is not BronzeReady")
if upload.get("bronze_status") != "BronzeReady" or upload.get("idempotent_reuse") is not True:
    raise SystemExit("same-run upload did not reuse immutable Bronze objects")
if verify.get("bronze_status") != "BronzeReady" or verify.get("verified") is not True:
    raise SystemExit("same-run readback verification failed")
if upload.get("manifest_uri") != verify.get("manifest_uri"):
    raise SystemExit("upload and verify manifest URI differ")
if upload.get("sha256") != verify.get("sha256"):
    raise SystemExit("upload and verify checksum differ")

print(
    "USG-06 live smoke passed: "
    f"run_id={summary['run_id']} "
    "window=[2023-01-01T00:00:00Z,2023-01-04T00:00:00Z) "
    f"manifest_uri={verify['manifest_uri']} "
    f"sha256={verify['sha256']} "
    f"record_count={verify['record_count_estimate']} "
    "rerun_reused=true"
)
PY
