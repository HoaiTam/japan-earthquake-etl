#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env.example"}
failed=0

fail() {
    printf 'USG-06 static check failed: %s\n' "$1" >&2
    failed=1
}

for relative_path in \
    compose/airflow/Dockerfile \
    compose/airflow/usgs-runner.sh \
    compose/airflow/usgs-live-smoke.sh \
    docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md \
    spark/src/main/java/ie212/earthquake/spark/usgs/MinioBronzeObjectStore.java \
    spark/src/main/java/ie212/earthquake/spark/usgs/UsgsIngestRunner.java \
    spark/src/test/java/ie212/earthquake/spark/usgs/MinioBronzeObjectStoreTest.java \
    spark/src/test/java/ie212/earthquake/spark/usgs/UsgsIngestRunnerTest.java
do
    [ -f "$project_root/$relative_path" ] || fail "missing $relative_path"
done

for executable_path in \
    compose/airflow/usgs-runner.sh \
    compose/airflow/usgs-live-smoke.sh \
    scripts/smoke-usgs-live.sh
do
    [ -x "$project_root/$executable_path" ] || fail "$executable_path must be executable"
done

for marker in \
    '<minio.version>9.0.3</minio.version>' \
    '<jackson.version>2.21.7</jackson.version>' \
    '<okhttp.version>5.3.2</okhttp.version>' \
    '<shadedClassifierName>runner</shadedClassifierName>' \
    'ie212.earthquake.spark.usgs.UsgsIngestRunner'
do
    grep -F -- "$marker" "$project_root/pom.xml" "$project_root/spark/pom.xml" >/dev/null || \
        fail "missing Maven runner marker: $marker"
done

for marker in \
    'USG-06 live Bronze runbook' \
    'rerun_reused=true' \
    'offset=1'
do
    grep -F -- "$marker" "$project_root/docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md" >/dev/null || \
        fail "missing live runbook marker: $marker"
done

for marker in \
    'japan-earthquake-etl/airflow:3.3.2-python3.13-java17' \
    'USGS_INGEST_RUNNER_COMMAND: ${USGS_INGEST_RUNNER_COMMAND:-/opt/pipeline/bin/usgs-ingest-runner}' \
    'MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:?MINIO_ACCESS_KEY is required}' \
    'source: ./compose/airflow/usgs-live-smoke.sh' \
    'usgs-live-smoke:'
do
    grep -F -- "$marker" "$compose_file" >/dev/null || \
        fail "missing Compose live-runner marker: $marker"
done

if [ "$failed" -ne 0 ]; then
    exit 1
fi

"$project_root/mvnw" --batch-mode --no-transfer-progress -pl spark -am test
python3 -m unittest discover -s "$project_root/airflow/tests" -p 'test_*.py'

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
    docker compose --env-file "$env_file" -f "$compose_file" --profile live config --quiet
else
    fail 'Docker Compose is required to validate the live profile'
fi

if grep -R -E --exclude='*.md' --exclude='*.xml' \
    '(AKIA[0-9A-Z]{16}|-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----)' \
    "$project_root/compose/airflow" \
    "$project_root/spark/src/main/java/ie212/earthquake/spark/usgs" >/dev/null; then
    fail 'runner files contain a credential/private-key pattern'
fi

if [ "$failed" -ne 0 ]; then
    exit 1
fi

printf 'USG-06 static check passed.\n'
