#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env"}
wait_timeout=${FOUNDATION_WAIT_TIMEOUT_SECONDS:-300}
failed=0

if [ "$#" -ne 0 ]; then
    printf 'Usage: ENV_FILE=/path/to/.env %s\n' "$0" >&2
    exit 2
fi

if [ ! -f "$env_file" ]; then
    printf 'ERROR: local environment file does not exist: %s\n' "$env_file" >&2
    printf 'Create it from .env.example and replace every change-me-* value.\n' >&2
    exit 1
fi

case "$wait_timeout" in
    ""|0|*[!0-9]*)
        printf 'ERROR: FOUNDATION_WAIT_TIMEOUT_SECONDS must be a positive integer.\n' >&2
        exit 1
        ;;
esac

compose() {
    docker compose --env-file "$env_file" -f "$compose_file" "$@"
}

show_failure_context() {
    printf '%s\n' \
        'Inspect service state with:' \
        "  docker compose --env-file $env_file -f $compose_file ps -a" \
        'Inspect one dependency chain before restarting or changing data:' \
        "  docker compose --env-file $env_file -f $compose_file logs --tail=100 <service>"
}

run_step() {
    label=$1
    shift

    printf '[RUN] %s\n' "$label"
    if "$@"; then
        printf '[PASS] %s\n' "$label"
        return
    fi

    printf '[FAIL] %s\n' "$label" >&2
    compose ps -a || true
    show_failure_context >&2
    exit 1
}

report_pass() {
    printf '[PASS] %s\n' "$1"
}

report_fail() {
    printf '[FAIL] %s\n' "$1" >&2
    failed=1
}

container_id() {
    compose ps -q "$1" 2>/dev/null | tail -n 1
}

all_container_id() {
    compose ps -a -q "$1" 2>/dev/null | tail -n 1
}

resolve_volume() {
    logical_name=$1
    docker volume ls \
        --filter "label=com.docker.compose.project=$project_name" \
        --filter "label=com.docker.compose.volume=$logical_name" \
        --format '{{.Name}}'
}

check_init_service() {
    service=$1
    id=$(all_container_id "$service")
    if [ -z "$id" ]; then
        report_fail "$service container exists"
        return
    fi

    state=$(docker inspect --format '{{.State.Status}}' "$id")
    exit_code=$(docker inspect --format '{{.State.ExitCode}}' "$id")
    if [ "$state" = "exited" ] && [ "$exit_code" = "0" ]; then
        report_pass "$service completed with exit code 0"
    else
        report_fail "$service must be exited/0 (actual: $state/$exit_code)"
    fi
}

check_service_health() {
    service=$1
    id=$(container_id "$service")
    if [ -z "$id" ]; then
        report_fail "$service container is running"
        return
    fi

    health=$(docker inspect --format \
        '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "$id")
    if [ "$health" = "healthy" ]; then
        report_pass "$service health=healthy"
    else
        report_fail "$service health must be healthy (actual: $health)"
    fi

    attached_networks=$(docker inspect --format \
        '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' \
        "$id")
    if printf '%s\n' "$attached_networks" | grep -Fxq "$network_name"; then
        report_pass "$service attached to network $network_name"
    else
        report_fail "$service must attach to network $network_name"
    fi

    startup_log=$(compose logs --no-color --tail=200 "$service" 2>&1 || true)
    if [ -z "$startup_log" ]; then
        report_fail "$service startup log is non-empty"
        return
    fi

    if printf '%s\n' "$startup_log" | grep -Eiq \
        '(^|[[:space:]])(fatal|panic)([[:space:]:]|$)|Exception in thread "main"|dependency failed to start'; then
        report_fail "$service startup log contains a fatal marker"
    else
        report_pass "$service startup log collected without fatal markers"
    fi
}

check_volume() {
    logical_name=$1
    expected_lifecycle=$2
    physical_names=$(resolve_volume "$logical_name")
    count=$(printf '%s\n' "$physical_names" | awk 'NF { count++ } END { print count + 0 }')

    if [ "$count" -ne 1 ]; then
        report_fail "$logical_name resolves to one project volume (actual: $count)"
        return
    fi

    physical_name=$physical_names
    lifecycle=$(docker volume inspect "$physical_name" \
        --format '{{index .Labels "com.japan-earthquake-etl.lifecycle"}}')
    if [ "$lifecycle" = "$expected_lifecycle" ]; then
        report_pass "$logical_name exists with lifecycle=$expected_lifecycle"
    else
        report_fail "$logical_name lifecycle must be $expected_lifecycle (actual: $lifecycle)"
    fi
}

check_mount() {
    service=$1
    logical_name=$2
    target=$3
    id=$(container_id "$service")
    physical_name=$(resolve_volume "$logical_name")

    if [ -z "$id" ] || [ -z "$physical_name" ]; then
        report_fail "$service mount $logical_name at $target"
        return
    fi

    mounts=$(docker inspect --format \
        '{{range .Mounts}}{{printf "%s|%s\n" .Name .Destination}}{{end}}' \
        "$id")
    if printf '%s\n' "$mounts" | grep -Fqx "$physical_name|$target"; then
        report_pass "$service mounts $logical_name at $target"
    else
        report_fail "$service must mount $logical_name at $target"
    fi
}

run_step "FND-01 static checklist" \
    env ENV_FILE="$env_file" COMPOSE_FILE="$compose_file" \
    "$project_root/scripts/check-foundation.sh" --require-local

run_step "Build pinned MinIO and Spark images" \
    compose build minio spark-master

run_step "Start all required foundation services" \
    compose up -d --wait --wait-timeout "$wait_timeout" --no-build \
    minio \
    airflow-postgres \
    airflow-api-server \
    airflow-scheduler \
    airflow-dag-processor \
    spark-master \
    spark-worker \
    iceberg-rest \
    trino

run_step "MinIO read/write smoke" \
    compose --profile smoke run --rm --no-deps minio-smoke
run_step "Airflow DAG smoke" \
    compose --profile smoke run --rm --no-deps airflow-smoke
run_step "Spark submit smoke" \
    compose --profile smoke run --rm --no-deps --use-aliases spark-client
run_step "Trino Iceberg query smoke" \
    compose --profile smoke run --rm --no-deps trino-smoke

project_name=$(compose config | awk '$1 == "name:" { print $2; exit }')
if [ -z "$project_name" ]; then
    printf 'ERROR: could not resolve the Compose project name.\n' >&2
    exit 1
fi

network_names=$(docker network ls \
    --filter "label=com.docker.compose.project=$project_name" \
    --filter 'label=com.docker.compose.network=pipeline' \
    --format '{{.Name}}')
network_count=$(printf '%s\n' "$network_names" | awk 'NF { count++ } END { print count + 0 }')
if [ "$network_count" -ne 1 ]; then
    printf 'ERROR: pipeline network must resolve exactly once (actual: %s).\n' \
        "$network_count" >&2
    exit 1
fi
network_name=$network_names
report_pass "pipeline network resolved as $network_name"

for service in \
    minio \
    airflow-postgres \
    airflow-api-server \
    airflow-scheduler \
    airflow-dag-processor \
    spark-master \
    spark-worker \
    iceberg-rest \
    trino
do
    check_service_health "$service"
done

check_init_service minio-init
check_init_service airflow-init

check_volume pipeline_staging transient
check_volume airflow_logs operational
check_volume airflow_db_data durable
check_volume minio_data durable
check_volume iceberg_catalog_data durable

check_mount minio minio_data /data
check_mount airflow-postgres airflow_db_data /var/lib/postgresql/data
check_mount airflow-api-server airflow_logs /opt/airflow/logs
check_mount airflow-api-server pipeline_staging /opt/pipeline/staging
check_mount spark-worker pipeline_staging /opt/pipeline/staging
check_mount iceberg-rest iceberg_catalog_data /home/iceberg

if [ "$failed" -ne 0 ]; then
    printf 'FND-01 environment smoke failed.\n' >&2
    show_failure_context >&2
    exit 1
fi

printf '%s\n' \
    'FND-01 environment smoke passed: 9 services healthy, 2 init services exited 0, 1 network and 5 volumes verified.' \
    'Foundation services and volumes remain available for debugging and downstream work.'
