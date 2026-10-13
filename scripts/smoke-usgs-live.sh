#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env"}

if [ ! -f "$env_file" ]; then
    printf 'USG-06 live smoke requires a local env file: %s\n' "$env_file" >&2
    exit 1
fi

ENV_FILE="$env_file" "$project_root/scripts/check-config.sh" --require-local

ENV_FILE="$env_file" COMPOSE_FILE="$compose_file" \
    python3 "$project_root/scripts/docker-runtime.py" check --component usgs

docker compose \
    --env-file "$env_file" \
    -f "$compose_file" \
    --profile live \
    run --rm --no-deps --pull never usgs-live-smoke

printf 'USG-06 live smoke completed; services and volumes were kept for review.\n'
