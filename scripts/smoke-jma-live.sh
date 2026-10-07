#!/usr/bin/env sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env"}
ENV_FILE="$env_file" "$project_root/scripts/check-config.sh" --require-local
"$project_root/scripts/check-real-sample-catalog.sh"
docker compose --env-file "$env_file" -f "$compose_file" \
    up -d --build --wait --wait-timeout 300 airflow-api-server airflow-scheduler airflow-dag-processor
docker compose --env-file "$env_file" -f "$compose_file" --profile live \
    run --rm --no-deps jma-live-qa
printf 'JMA-05 live QA completed; scoped evidence, services and all volumes were kept.\n'
