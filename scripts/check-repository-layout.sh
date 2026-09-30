#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)

required_directories="
airflow/dags
airflow/tests
compose
compose/airflow
compose/minio
compose/spark
compose/trino
scripts
spark/src/main/java
spark/src/test/java
spark/src/test/resources/fixtures
tests/fixtures
tests/integration
trino/catalog
"

required_files="
.dockerignore
.gitattributes
.env.example
README.md
.mvn/wrapper/maven-wrapper.properties
airflow/README.md
airflow/dags/README.md
airflow/dags/afl_01_smoke.py
airflow/tests/README.md
airflow/tests/test_smoke_dag_contract.py
compose/README.md
compose/airflow/smoke.sh
compose/minio/Dockerfile
compose/minio/init.sh
compose/minio/smoke.sh
compose/spark/Dockerfile
compose/spark/smoke.sh
compose/trino/smoke.sh
compose.yaml
docs/specs/COMPOSE_FOUNDATION.md
docs/specs/CONFIGURATION_AND_SECRETS.md
docs/specs/AIRFLOW_LOCAL.md
docs/specs/FOUNDATION_SMOKE.md
docs/specs/MINIO_STORAGE.md
docs/specs/ICEBERG_TRINO.md
docs/specs/REPOSITORY_LAYOUT.md
docs/specs/SPARK_STANDALONE.md
docs/specs/SOURCE_COVERAGE.md
docs/specs/BRONZE_STORAGE_CONTRACT.md
docs/specs/SILVER_GOLD_DATA_MODEL.md
scripts/README.md
scripts/check-airflow.sh
scripts/check-compose.sh
scripts/check-config.sh
scripts/check-foundation.sh
scripts/check-minio.sh
scripts/check-query.sh
scripts/check-spark.sh
scripts/check-source-coverage.sh
scripts/check-bronze-contract.sh
scripts/check-data-model-contract.sh
scripts/build-shared-fixtures.sh
scripts/check-shared-fixtures.sh
scripts/smoke-airflow.sh
scripts/smoke-foundation.sh
scripts/smoke-minio.sh
scripts/smoke-query.sh
scripts/smoke-spark.sh
pom.xml
mvnw
mvnw.cmd
spark/README.md
spark/pom.xml
spark/src/main/java/vn/edu/uit/ie212/earthquake/spark/HelloWorldJob.java
spark/src/test/java/vn/edu/uit/ie212/earthquake/spark/HelloWorldJobTest.java
tests/README.md
tests/fixtures/README.md
tests/fixtures/TEST_MATRIX.md
tests/fixtures/cases.json
tests/fixtures/SHA256SUMS
tests/integration/README.md
trino/README.md
trino/catalog/README.md
trino/catalog/iceberg.properties
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
