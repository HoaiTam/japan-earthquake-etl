#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env.example"}
catalog_file="$project_root/trino/catalog/iceberg.properties"
iceberg_image='apache/iceberg-rest-fixture:1.10.1@sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f'
trino_image='trinodb/trino:483@sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534'
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
    compose/trino/smoke.sh \
    trino/catalog/iceberg.properties
do
    if [ ! -f "$project_root/$relative_path" ]; then
        report_error "missing QRY-01 asset: $relative_path"
    fi
done

for executable_path in compose/trino/smoke.sh; do
    if [ ! -x "$project_root/$executable_path" ]; then
        report_error "$executable_path must be executable"
    fi
done

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
    printf 'QRY-01 query static check failed.\n' >&2
    exit 1
fi

for expected_line in \
    'connector.name=iceberg' \
    'iceberg.catalog.type=rest' \
    'iceberg.rest-catalog.uri=${ENV:ICEBERG_CATALOG_URI}' \
    'iceberg.rest-catalog.warehouse=${ENV:WAREHOUSE_PATH}' \
    'iceberg.rest-catalog.vended-credentials-enabled=false' \
    'fs.s3.enabled=true' \
    's3.endpoint=${ENV:MINIO_ENDPOINT}' \
    's3.region=${ENV:S3_REGION}' \
    's3.path-style-access=true' \
    's3.aws-access-key=${ENV:MINIO_ACCESS_KEY}' \
    's3.aws-secret-key=${ENV:MINIO_SECRET_KEY}'
do
    if ! grep -Fxq "$expected_line" "$catalog_file"; then
        report_error "Trino Iceberg catalog is missing required property: $expected_line"
    fi
done

if grep -Eq '^s3[.]aws-(access|secret)-key=[^$]' "$catalog_file"; then
    report_error "Trino catalog must not contain a literal S3 credential"
fi

if grep -Fq 'change-me-' "$catalog_file"; then
    report_error "Trino catalog must reference environment secrets, not placeholders"
fi

docker compose --env-file "$env_file" -f "$compose_file" config --quiet
docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --quiet

services=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --services)
rendered=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config)

for service in iceberg-rest trino trino-smoke; do
    if ! printf '%s\n' "$services" | grep -Fxq "$service"; then
        report_error "missing Compose service: $service"
    fi
done

iceberg_block=$(service_block "$rendered" iceberg-rest)
trino_block=$(service_block "$rendered" trino)
smoke_block=$(service_block "$rendered" trino-smoke)

require_fixed "$iceberg_block" "image: $iceberg_image" \
    "iceberg-rest must use the reviewed digest-pinned image"
require_fixed "$iceberg_block" "condition: service_completed_successfully" \
    "iceberg-rest must wait for successful MinIO bootstrap"
require_fixed "$iceberg_block" "http://localhost:8181/v1/config" \
    "iceberg-rest must check the REST config endpoint"
require_fixed "$iceberg_block" "source: iceberg_catalog_data" \
    "iceberg-rest must use the durable catalog volume"
require_fixed "$iceberg_block" "target: /home/iceberg" \
    "iceberg catalog state must persist under /home/iceberg"
require_fixed "$iceberg_block" "read_only: true" \
    "iceberg-rest must use a read-only root filesystem"
require_fixed "$iceberg_block" "exec,mode=1777" \
    "iceberg-rest tmpfs must allow sqlite-jdbc to load its native library"
if printf '%s\n' "$iceberg_block" | grep -Fq 'published:'; then
    report_error "Iceberg REST Catalog must not publish a host port"
fi

require_fixed "$trino_block" "image: $trino_image" \
    "trino must use the reviewed digest-pinned image"
require_fixed "$trino_block" "condition: service_healthy" \
    "trino must wait for a healthy Iceberg REST Catalog"
require_fixed "$trino_block" "condition: service_completed_successfully" \
    "trino must wait for successful MinIO bootstrap"
require_fixed "$trino_block" "host_ip: 127.0.0.1" \
    "Trino must bind its host port to loopback"
require_fixed "$trino_block" "target: 8080" \
    "Trino must use container port 8080"
require_fixed "$trino_block" "/trino/catalog" \
    "Trino must mount the reviewed catalog directory"
require_fixed "$trino_block" "target: /etc/trino/catalog" \
    "Trino catalog directory must mount at /etc/trino/catalog"
require_fixed "$trino_block" "/usr/lib/trino/bin/health-check" \
    "Trino must use its official health check"

require_fixed "$smoke_block" "image: $trino_image" \
    "trino-smoke must use the same reviewed Trino image"
require_fixed "$smoke_block" "condition: service_healthy" \
    "trino-smoke must wait for healthy Trino"
require_fixed "$smoke_block" "target: /opt/trino/smoke/run.sh" \
    "trino-smoke must mount the reviewed query runner"
require_fixed "$smoke_block" 'restart: "no"' \
    "trino-smoke must be a one-shot service"
require_fixed "$smoke_block" "exec,mode=1777" \
    "trino-smoke tmpfs must allow the Trino CLI native helper"

if grep -Eq 'image:[[:space:]]+(apache/iceberg-rest-fixture|trinodb/trino):(latest|edge)(@|[[:space:]]|$)' \
    "$compose_file"; then
    report_error "QRY-01 images must not use mutable latest/edge tags"
fi

if [ "$failed" -ne 0 ]; then
    printf 'QRY-01 query static check failed.\n' >&2
    exit 1
fi

printf 'QRY-01 query static check passed.\n'
