#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env.example"}
airflow_image=japan-earthquake-etl/airflow:3.3.2-python3.13-java17
airflow_init_image=apache/airflow:3.3.2-python3.13
postgres_image=postgres:16.12-bookworm
failed=0

report_error() {
    printf 'ERROR: %s\n' "$1" >&2
    failed=1
}

service_block() {
    rendered_config=$1
    target_service=$2

    printf '%s\n' "$rendered_config" | awk -v target_service="$target_service" '
        $0 == "  " target_service ":" { in_service = 1 }
        in_service && /^  [A-Za-z0-9_-]+:$/ && $0 != "  " target_service ":" { exit }
        in_service { print }
    '
}

require_fixed() {
    content=$1
    expected=$2
    message=$3

    if ! printf '%s\n' "$content" | grep -Fq "$expected"; then
        report_error "$message"
    fi
}

for relative_path in \
    airflow/dags/afl_01_smoke.py \
    airflow/dags/usg_04_usgs_ingest.py \
    airflow/dags/usgs_ingest_runtime.py \
    airflow/tests/test_smoke_dag_contract.py \
    airflow/tests/test_usg_04_dag_contract.py \
    airflow/tests/test_usgs_ingest_runtime.py \
    compose/airflow/Dockerfile \
    compose/airflow/usgs-runner.sh \
    compose/airflow/usgs-live-smoke.sh \
    compose/airflow/smoke.sh
do
    if [ ! -f "$project_root/$relative_path" ]; then
        report_error "missing AFL-01 asset: $relative_path"
    fi
done

for executable_path in \
    compose/airflow/smoke.sh \
    compose/airflow/usgs-runner.sh \
    compose/airflow/usgs-live-smoke.sh
do
    if [ ! -x "$project_root/$executable_path" ]; then
        report_error "$executable_path must be executable"
    fi
done

if [ -f "$project_root/airflow/dags/usg_04_usgs_ingest.py" ]; then
    usgs_dag_source=$(cat "$project_root/airflow/dags/usg_04_usgs_ingest.py")
    for expected in \
        'usg_04_usgs_ingest' \
        'group_id="usgs_ingest"' \
        'task_id="resolve_interval"' \
        'task_id="fetch"' \
        'task_id="validate"' \
        'task_id="upload"' \
        'task_id="verify"' \
        'task_id="bronze_ready_gate"' \
        'task_id="run_summary"'; do
        if ! printf '%s\n' "$usgs_dag_source" | grep -Fq "$expected"; then
            report_error "USG-04 DAG is missing required contract marker: $expected"
        fi
    done
fi

if [ -f "$project_root/airflow/dags/usgs_ingest_runtime.py" ]; then
    usgs_runtime_source=$(cat "$project_root/airflow/dags/usgs_ingest_runtime.py")
    for expected in \
        'USGS_INGEST_RUNNER_COMMAND' \
        'PUBLIC_RESULT_KEYS' \
        'USGS_INGEST_DRY_RUN' \
        'BronzeReady'; do
        if ! printf '%s\n' "$usgs_runtime_source" | grep -Fq "$expected"; then
            report_error "USG-04 runtime boundary is missing required marker: $expected"
        fi
    done
fi

if [ ! -f "$compose_file" ]; then
    report_error "compose file does not exist: $compose_file"
fi

if [ ! -f "$env_file" ]; then
    report_error "environment file does not exist: $env_file"
fi

if ! command -v docker >/dev/null 2>&1; then
    report_error "docker CLI is not installed"
elif ! docker compose version >/dev/null 2>&1; then
    report_error "Docker Compose plugin is not available"
fi

if [ "$failed" -ne 0 ]; then
    printf 'AFL-01 Airflow static check failed.\n' >&2
    exit 1
fi

python3 -m unittest discover \
    -s "$project_root/airflow/tests" \
    -p 'test_*.py'

docker compose --env-file "$env_file" -f "$compose_file" config --quiet
docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --quiet

services=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --services)
rendered=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config)

for service in \
    airflow-postgres \
    airflow-init \
    airflow-api-server \
    airflow-scheduler \
    airflow-dag-processor \
    airflow-smoke
do
    if ! printf '%s\n' "$services" | grep -Fxq "$service"; then
        report_error "missing Compose service: $service"
    fi
done

postgres_block=$(service_block "$rendered" airflow-postgres)
init_block=$(service_block "$rendered" airflow-init)
api_block=$(service_block "$rendered" airflow-api-server)
scheduler_block=$(service_block "$rendered" airflow-scheduler)
processor_block=$(service_block "$rendered" airflow-dag-processor)
smoke_block=$(service_block "$rendered" airflow-smoke)

require_fixed "$postgres_block" "image: $postgres_image" \
    "airflow-postgres must use the reviewed pinned image"
require_fixed "$postgres_block" "pg_isready" \
    "airflow-postgres must define a readiness healthcheck"
require_fixed "$postgres_block" "source: airflow_db_data" \
    "airflow-postgres must use the durable metadata volume"
require_fixed "$postgres_block" "target: /var/lib/postgresql/data" \
    "airflow_db_data must mount at the PostgreSQL data path"
if printf '%s\n' "$postgres_block" | grep -Fq "published:"; then
    report_error "Airflow PostgreSQL must not publish a host port"
fi

require_fixed "$init_block" "image: $airflow_init_image" \
    "airflow-init must use the reviewed Airflow image"
require_fixed "$init_block" "_AIRFLOW_DB_MIGRATE: \"true\"" \
    "airflow-init must migrate the metadata database"
require_fixed "$init_block" "_AIRFLOW_WWW_USER_CREATE: \"true\"" \
    "airflow-init must create the local admin user"
require_fixed "$init_block" "AIRFLOW_CONFIG: /tmp/airflow.cfg" \
    "airflow-init must write generated config to its writable tmpfs"
require_fixed "$init_block" "airflow db check" \
    "airflow-init must fail when the metadata database is not ready"
require_fixed "$init_block" "restart: \"no\"" \
    "airflow-init must be a one-shot service"

for block_name in api scheduler processor smoke; do
    eval "block=\${${block_name}_block}"
    require_fixed "$block" "image: $airflow_image" \
        "airflow-$block_name must use the reviewed Airflow image"
    require_fixed "$block" "AIRFLOW__CORE__EXECUTOR: LocalExecutor" \
        "airflow-$block_name must use LocalExecutor"
    require_fixed "$block" "source: airflow_logs" \
        "airflow-$block_name must mount airflow_logs"
    require_fixed "$block" "source: pipeline_staging" \
        "airflow-$block_name must mount pipeline_staging"
done

require_fixed "$api_block" "host_ip: 127.0.0.1" \
    "Airflow UI/API must bind to loopback"
require_fixed "$api_block" "target: 8080" \
    "Airflow API server must use container port 8080"
require_fixed "$api_block" "/api/v2/monitor/health" \
    "Airflow API server must check the official health endpoint"
require_fixed "$api_block" "condition: service_completed_successfully" \
    "Airflow API server must wait for successful initialization"

require_fixed "$scheduler_block" "exec airflow jobs check --job-type SchedulerJob" \
    "Airflow scheduler must expose a heartbeat-based healthcheck"
require_fixed "$scheduler_block" "timeout: 30s" \
    "Airflow scheduler healthcheck must allow a full CLI startup"
require_fixed "$scheduler_block" "condition: service_completed_successfully" \
    "Airflow scheduler must wait for successful initialization"

require_fixed "$processor_block" "exec airflow jobs check --job-type DagProcessorJob" \
    "Airflow DAG processor must expose a heartbeat-based healthcheck"
require_fixed "$processor_block" "timeout: 30s" \
    "Airflow DAG processor healthcheck must allow a full CLI startup"
require_fixed "$processor_block" "condition: service_completed_successfully" \
    "Airflow DAG processor must wait for successful initialization"

require_fixed "$smoke_block" "target: /opt/airflow/smoke/run.sh" \
    "airflow-smoke must mount the reviewed smoke runner"
require_fixed "$smoke_block" "condition: service_healthy" \
    "airflow-smoke must wait for healthy long-running components"

if grep -Eq 'image:[[:space:]]+(apache/airflow|postgres):(latest|edge)([[:space:]]|$)' \
    "$compose_file"; then
    report_error "Airflow and PostgreSQL images must not use mutable latest/edge tags"
fi

if ! grep -Fq 'AIRFLOW__CORE__LOAD_EXAMPLES: "false"' "$compose_file"; then
    report_error "Airflow example DAGs must stay disabled"
fi

if ! grep -Fq 'AIRFLOW__API_AUTH__JWT_SECRET: ${AIRFLOW_API_JWT_SECRET:' \
    "$compose_file"; then
    report_error "Airflow API JWT secret must come from the external environment"
fi

if [ "$failed" -ne 0 ]; then
    printf 'AFL-01 Airflow static check failed.\n' >&2
    exit 1
fi

printf 'AFL-01 Airflow static check passed.\n'
