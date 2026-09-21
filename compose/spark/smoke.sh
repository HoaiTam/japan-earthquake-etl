#!/usr/bin/env bash

set -euo pipefail

job_jar=/opt/spark/jobs/japan-earthquake-etl.jar
main_class=vn.edu.uit.ie212.earthquake.spark.HelloWorldJob
output_file=$(mktemp)

cleanup() {
    rm -f "$output_file"
}
trap cleanup EXIT

if [ ! -r "$job_jar" ]; then
    printf 'ERROR: Spark smoke JAR is missing or unreadable: %s\n' "$job_jar" >&2
    exit 1
fi

master_status=$(wget -qO- http://spark-master:8080/json/)
if ! printf '%s\n' "$master_status" \
    | grep -Eq '"state"[[:space:]]*:[[:space:]]*"ALIVE"'; then
    printf 'ERROR: Spark master does not report an ALIVE worker.\n' >&2
    exit 1
fi

set +e
/opt/spark/bin/spark-submit \
    --master "$SPARK_MASTER_URL" \
    --deploy-mode client \
    --name spk-01-hello-world \
    --class "$main_class" \
    --driver-memory "$SPARK_DRIVER_MEMORY" \
    --executor-memory "$SPARK_EXECUTOR_MEMORY" \
    --conf spark.driver.bindAddress=0.0.0.0 \
    --conf spark.driver.host=spark-client \
    --conf spark.cores.max="$SPARK_WORKER_CORES" \
    --conf spark.executor.cores="$SPARK_WORKER_CORES" \
    --conf spark.executor.instances=1 \
    --conf spark.ui.enabled=false \
    "$job_jar" 2>&1 | tee "$output_file"
submit_status=${PIPESTATUS[0]}
set -e

if [ "$submit_status" -ne 0 ]; then
    printf 'ERROR: spark-submit exited with code %s.\n' "$submit_status" >&2
    exit "$submit_status"
fi

if ! grep -Fq \
    'event=spark_hello_world_success' \
    "$output_file"; then
    printf 'ERROR: Spark success event is missing from job output.\n' >&2
    exit 1
fi

if ! grep -Fq \
    'master=spark://spark-master:7077 record_count=10 id_sum=45' \
    "$output_file"; then
    printf 'ERROR: Spark smoke result does not match the expected cluster summary.\n' >&2
    exit 1
fi

printf '%s\n' \
    'SPK-01 Spark smoke passed: worker ALIVE, spark-submit exit code 0.'
