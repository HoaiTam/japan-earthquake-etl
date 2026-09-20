#!/usr/bin/env bash

set -euo pipefail

dag_id=afl_01_smoke
run_suffix=$(python -c 'import uuid; print(uuid.uuid4().hex[:12])')
run_id="afl01-smoke-$(date -u +%Y%m%dT%H%M%SZ)-${run_suffix}"
max_attempts=60
attempt=1

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
if not any(item.get("dag_id") == "afl_01_smoke" for item in dags):
    raise SystemExit("afl_01_smoke is missing from the serialized DAG list")
PY

airflow dags trigger "$dag_id" --run-id "$run_id" >/dev/null

while [ "$attempt" -le "$max_attempts" ]; do
    state=$(airflow dags state "$dag_id" "$run_id" 2>/dev/null | tail -n 1 | tr -d '\r')
    case "$state" in
        success)
            printf 'AFL-01 Airflow smoke DAG passed: dag_id=%s run_id=%s\n' "$dag_id" "$run_id"
            exit 0
            ;;
        failed)
            printf 'ERROR: Airflow smoke DAG failed: dag_id=%s run_id=%s\n' "$dag_id" "$run_id" >&2
            exit 1
            ;;
    esac

    sleep 2
    attempt=$((attempt + 1))
done

printf 'ERROR: Airflow smoke DAG did not finish after %s attempts: dag_id=%s run_id=%s\n' \
    "$max_attempts" "$dag_id" "$run_id" >&2
exit 1
