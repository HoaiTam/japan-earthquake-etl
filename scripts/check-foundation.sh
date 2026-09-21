#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
compose_env_file=${ENV_FILE:-"$project_root/.env.example"}
local_env_file=${ENV_FILE:-"$project_root/.env"}
require_local=0
failed=0

case "${1:-}" in
    "") ;;
    --require-local) require_local=1 ;;
    *)
        printf 'Usage: %s [--require-local]\n' "$0" >&2
        exit 2
        ;;
esac

report_error() {
    printf 'ERROR: %s\n' "$1" >&2
    failed=1
}

run_check() {
    label=$1
    shift

    printf '[RUN] %s\n' "$label"
    "$@"
}

required_executables="
scripts/check-repository-layout.sh
scripts/check-config.sh
scripts/check-compose.sh
scripts/check-minio.sh
scripts/check-airflow.sh
scripts/check-spark.sh
scripts/check-query.sh
scripts/smoke-foundation.sh
"

for relative_path in $required_executables; do
    if [ ! -x "$project_root/$relative_path" ]; then
        report_error "$relative_path must exist and be executable"
    fi
done

for relative_path in \
    docs/specs/FOUNDATION_SMOKE.md \
    compose.yaml
do
    if [ ! -f "$project_root/$relative_path" ]; then
        report_error "missing FND-01 asset: $relative_path"
    fi
done

for expected_name in \
    minio \
    airflow-postgres \
    airflow-api-server \
    airflow-scheduler \
    airflow-dag-processor \
    spark-master \
    spark-worker \
    iceberg-rest \
    trino \
    minio-smoke \
    airflow-smoke \
    spark-client \
    trino-smoke \
    pipeline \
    pipeline_staging \
    airflow_logs \
    airflow_db_data \
    minio_data \
    iceberg_catalog_data
do
    if ! grep -Fq "$expected_name" "$project_root/scripts/smoke-foundation.sh"; then
        report_error "smoke-foundation.sh is missing required runtime check: $expected_name"
    fi
done

if grep -Eq 'docker[[:space:]]+(compose[[:space:]]+)?(down[[:space:]]+-v|volume[[:space:]]+rm)|rm[[:space:]]+-rf' \
    "$project_root/scripts/smoke-foundation.sh"; then
    report_error "foundation smoke must not delete volumes or use recursive removal"
fi

if [ "$failed" -ne 0 ]; then
    printf 'FND-01 foundation static checklist failed.\n' >&2
    exit 1
fi

run_check "REP-01 repository layout" \
    "$project_root/scripts/check-repository-layout.sh"

if [ "$require_local" -eq 1 ]; then
    run_check "CFG-01 local configuration" \
        env ENV_FILE="$local_env_file" \
        "$project_root/scripts/check-config.sh" --require-local
else
    run_check "CFG-01 repository configuration" \
        "$project_root/scripts/check-config.sh"
fi

for task_and_script in \
    "CMP-01:scripts/check-compose.sh" \
    "MIO-01:scripts/check-minio.sh" \
    "AFL-01:scripts/check-airflow.sh" \
    "SPK-01:scripts/check-spark.sh" \
    "QRY-01:scripts/check-query.sh"
do
    task=${task_and_script%%:*}
    relative_script=${task_and_script#*:}
    run_check "$task static contract" \
        env ENV_FILE="$compose_env_file" COMPOSE_FILE="$compose_file" \
        "$project_root/$relative_script"
done

printf '%s\n' \
    'FND-01 foundation static checklist passed: repository, config, Compose, MinIO, Airflow, Spark and query contracts.'
