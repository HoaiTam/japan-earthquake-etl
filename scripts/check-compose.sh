#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env.example"}

required_networks="
pipeline
"

required_volumes="
pipeline_staging
airflow_logs
airflow_db_data
minio_data
iceberg_catalog_data
"

failed=0

report_error() {
    printf 'ERROR: %s\n' "$1" >&2
    failed=1
}

require_item() {
    available_items=$1
    expected_item=$2
    item_type=$3

    if ! printf '%s\n' "$available_items" | grep -Fxq "$expected_item"; then
        report_error "missing Compose $item_type: $expected_item"
    fi
}

volume_lifecycle() {
    rendered_config=$1
    target_volume=$2

    printf '%s\n' "$rendered_config" | awk -v target_volume="$target_volume" '
        /^volumes:$/ { in_volumes = 1; next }
        in_volumes && $0 == "  " target_volume ":" {
            in_target = 1
            next
        }
        in_target && /^  [A-Za-z0-9_-]+:$/ { exit }
        in_target && /com[.]japan-earthquake-etl[.]lifecycle:/ {
            sub(/^.*lifecycle:[[:space:]]*/, "")
            print
            exit
        }
    '
}

has_healthy_dependency() {
    rendered_config=$1

    printf '%s\n' "$rendered_config" | awk '
        $0 == "  compose-contract:" { in_service = 1; next }
        in_service && /^  [A-Za-z0-9_-]+:$/ { exit }
        in_service && /condition:[[:space:]]+service_healthy/ { found = 1 }
        END { exit(found ? 0 : 1) }
    '
}

service_has_pattern() {
    rendered_config=$1
    target_service=$2
    expected_pattern=$3

    printf '%s\n' "$rendered_config" | awk \
        -v target_service="$target_service" \
        -v expected_pattern="$expected_pattern" '
        $0 == "  " target_service ":" { in_service = 1; next }
        in_service && /^  [A-Za-z0-9_-]+:$/ { exit }
        in_service && $0 ~ expected_pattern { found = 1 }
        END { exit(found ? 0 : 1) }
    '
}

require_top_level_block() {
    rendered_config=$1
    block_name=$2

    if ! printf '%s\n' "$rendered_config" | grep -Fxq "$block_name:"; then
        report_error "missing Compose extension: $block_name"
    fi
}

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
    printf 'CMP-01 Docker Compose foundation check failed.\n' >&2
    exit 1
fi

docker compose --env-file "$env_file" -f "$compose_file" config --quiet
docker compose --env-file "$env_file" -f "$compose_file" \
    --profile validation config --quiet

networks=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile validation config --networks)
volumes=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile validation config --volumes)
services=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile validation config --services)
rendered=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile validation config)

for network in $required_networks; do
    require_item "$networks" "$network" "network"
done

for volume in $required_volumes; do
    require_item "$volumes" "$volume" "volume"
done

require_item "$services" "compose-contract" "validation service"
require_item "$services" "compose-health-contract" "validation service"

if ! has_healthy_dependency "$rendered"; then
    report_error "compose-contract must depend on compose-health-contract with service_healthy"
fi

if ! service_has_pattern "$rendered" compose-contract '^    deploy:$'; then
    report_error "compose-contract must apply the resource baseline"
fi

if ! service_has_pattern "$rendered" compose-health-contract '^    healthcheck:$'; then
    report_error "compose-health-contract must define a healthcheck"
fi

for extension in \
    x-runtime-defaults \
    x-resource-baseline \
    x-healthcheck-defaults \
    x-healthy-dependency
do
    require_top_level_block "$rendered" "$extension"
done

staging_lifecycle=$(volume_lifecycle "$rendered" pipeline_staging)
if [ "$staging_lifecycle" != "transient" ]; then
    report_error "pipeline_staging must have lifecycle label: transient"
fi

airflow_logs_lifecycle=$(volume_lifecycle "$rendered" airflow_logs)
if [ "$airflow_logs_lifecycle" != "operational" ]; then
    report_error "airflow_logs must have lifecycle label: operational"
fi

for volume in airflow_db_data minio_data iceberg_catalog_data; do
    lifecycle=$(volume_lifecycle "$rendered" "$volume")
    if [ "$lifecycle" != "durable" ]; then
        report_error "$volume must have lifecycle label: durable"
    fi
done

if [ "$failed" -ne 0 ]; then
    printf 'CMP-01 Docker Compose foundation check failed.\n' >&2
    exit 1
fi

printf 'CMP-01 Docker Compose foundation check passed.\n'
