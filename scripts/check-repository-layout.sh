#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

required_directories="
airflow/dags
airflow/tests
compose
scripts
spark/src/main/java
spark/src/test/java
spark/src/test/resources/fixtures
tests/fixtures
tests/integration
trino/catalog
"

required_files="
.env.example
README.md
airflow/README.md
airflow/dags/README.md
airflow/tests/README.md
compose/README.md
compose.yaml
docs/specs/COMPOSE_FOUNDATION.md
docs/specs/CONFIGURATION_AND_SECRETS.md
docs/specs/REPOSITORY_LAYOUT.md
scripts/README.md
scripts/check-compose.sh
scripts/check-config.sh
spark/README.md
tests/README.md
tests/fixtures/README.md
tests/integration/README.md
trino/README.md
trino/catalog/README.md
"

failed=0

for relative_path in $required_directories; do
    if [ ! -d "$project_root/$relative_path" ]; then
        printf 'Missing required directory: %s\n' "$relative_path" >&2
        failed=1
    fi
done

for relative_path in $required_files; do
    if [ ! -f "$project_root/$relative_path" ]; then
        printf 'Missing required file: %s\n' "$relative_path" >&2
        failed=1
    fi
done

if [ "$failed" -ne 0 ]; then
    printf 'REP-01 repository scaffold check failed.\n' >&2
    exit 1
fi

printf 'REP-01 repository scaffold check passed.\n'
