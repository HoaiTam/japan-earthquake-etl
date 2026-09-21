#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env"}

if [ "$#" -ne 0 ]; then
    printf 'Usage: ENV_FILE=/path/to/.env %s\n' "$0" >&2
    exit 2
fi

if [ ! -f "$env_file" ]; then
    printf 'ERROR: local environment file does not exist: %s\n' "$env_file" >&2
    printf 'Create it from .env.example and replace every change-me-* value.\n' >&2
    exit 1
fi

ENV_FILE="$env_file" "$project_root/scripts/check-config.sh" --require-local
ENV_FILE="$env_file" COMPOSE_FILE="$compose_file" \
    "$project_root/scripts/check-spark.sh"

docker compose --env-file "$env_file" -f "$compose_file" \
    build spark-master

docker compose --env-file "$env_file" -f "$compose_file" \
    up -d --no-build --wait spark-master spark-worker

docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke run --rm --no-deps --use-aliases spark-client

printf '%s\n' \
    'SPK-01 runtime smoke completed; Spark services and staging volume are preserved.'
