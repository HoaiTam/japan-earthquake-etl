#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env.example"}

server_image=japan-earthquake-etl/minio:RELEASE.2025-10-15T17-29-55Z
client_image=quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z
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

if [ ! -f "$compose_file" ]; then
    report_error "compose file does not exist: $compose_file"
fi

if [ ! -f "$env_file" ]; then
    report_error "environment file does not exist: $env_file"
fi

for relative_path in compose/minio/init.sh compose/minio/smoke.sh; do
    if [ ! -x "$project_root/$relative_path" ]; then
        report_error "$relative_path must exist and be executable"
    fi
done

if [ ! -f "$project_root/compose/minio/Dockerfile" ]; then
    report_error "compose/minio/Dockerfile must exist"
fi

if ! grep -Fq 'MINIO_RELEASE=RELEASE go run buildscripts/gen-ldflags.go' \
    "$project_root/compose/minio/Dockerfile"; then
    report_error "MinIO Dockerfile must embed upstream release metadata"
fi

if ! grep -Fq '/out/minio --version | grep -F "$MINIO_VERSION"' \
    "$project_root/compose/minio/Dockerfile"; then
    report_error "MinIO Dockerfile must verify the built binary version"
fi

if ! command -v docker >/dev/null 2>&1; then
    report_error "docker CLI is not installed"
elif ! docker compose version >/dev/null 2>&1; then
    report_error "Docker Compose plugin is not available"
fi

if [ "$failed" -ne 0 ]; then
    printf 'MIO-01 MinIO static check failed.\n' >&2
    exit 1
fi

docker compose --env-file "$env_file" -f "$compose_file" config --quiet
docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --quiet

services=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --services)
rendered=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config)

for service in minio minio-init minio-smoke; do
    if ! printf '%s\n' "$services" | grep -Fxq "$service"; then
        report_error "missing Compose service: $service"
    fi
done

minio_block=$(service_block "$rendered" minio)
init_block=$(service_block "$rendered" minio-init)
smoke_block=$(service_block "$rendered" minio-smoke)

require_fixed "$minio_block" "image: $server_image" \
    "minio must use the reviewed pinned server image"
require_fixed "$minio_block" "MINIO_COMMIT: 9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a" \
    "minio source build must pin the reviewed upstream commit"
require_fixed "$minio_block" "MINIO_VERSION: RELEASE.2025-10-15T17-29-55Z" \
    "minio source build must pin the reviewed release"
require_fixed "$minio_block" "dockerfile: Dockerfile" \
    "minio must build from the reviewed Dockerfile"
require_fixed "$minio_block" "pull_policy: build" \
    "minio must build locally instead of pulling a nonexistent server image"
require_fixed "$minio_block" "http://localhost:9000/minio/health/live" \
    "minio must check its liveness endpoint"
require_fixed "$minio_block" "source: minio_data" \
    "minio must use the durable minio_data volume"
require_fixed "$minio_block" "target: /data" \
    "minio_data must mount at /data"
require_fixed "$minio_block" "host_ip: 127.0.0.1" \
    "MinIO Console must bind to loopback"
require_fixed "$minio_block" "target: 9001" \
    "MinIO Console must use container port 9001"

if printf '%s\n' "$minio_block" | grep -Fq "published: \"9000\""; then
    report_error "MinIO S3 API port 9000 must not be published to the host"
fi

require_fixed "$init_block" "image: $client_image" \
    "minio-init must use the reviewed pinned client image"
require_fixed "$init_block" "condition: service_healthy" \
    "minio-init must wait for healthy MinIO"
require_fixed "$init_block" "target: /opt/minio/init.sh" \
    "minio-init must mount its reviewed bootstrap script"
require_fixed "$init_block" "read_only: true" \
    "minio-init must run with a read-only root filesystem"

require_fixed "$smoke_block" "image: $client_image" \
    "minio-smoke must use the reviewed pinned client image"
require_fixed "$smoke_block" "condition: service_completed_successfully" \
    "minio-smoke must wait for successful bootstrap"
require_fixed "$smoke_block" "target: /opt/minio/smoke.sh" \
    "minio-smoke must mount its reviewed smoke script"

if grep -Eq 'image:[[:space:]]+([^/]+/)?minio/(minio|mc):(latest|edge)([[:space:]]|$)' \
    "$compose_file"; then
    report_error "MinIO images must not use mutable latest/edge tags"
fi

if grep -Eq '(^|[[:space:]])sleep([[:space:]]|$)' \
    "$project_root/compose/minio/init.sh"; then
    report_error "MinIO bootstrap must use readiness, not fixed sleep"
fi

if [ "$failed" -ne 0 ]; then
    printf 'MIO-01 MinIO static check failed.\n' >&2
    exit 1
fi

printf 'MIO-01 MinIO static check passed.\n'
