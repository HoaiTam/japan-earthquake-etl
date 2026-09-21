#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env.example"}
spark_image=japan-earthquake-etl/spark:3.5.9-java17
job_jar="$project_root/spark/target/japan-earthquake-etl.jar"
main_class_path=vn/edu/uit/ie212/earthquake/spark/HelloWorldJob.class
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
    .dockerignore \
    .mvn/wrapper/maven-wrapper.properties \
    mvnw \
    mvnw.cmd \
    pom.xml \
    spark/pom.xml \
    spark/src/main/java/vn/edu/uit/ie212/earthquake/spark/HelloWorldJob.java \
    spark/src/test/java/vn/edu/uit/ie212/earthquake/spark/HelloWorldJobTest.java \
    compose/spark/Dockerfile \
    compose/spark/smoke.sh
do
    if [ ! -f "$project_root/$relative_path" ]; then
        report_error "missing SPK-01 asset: $relative_path"
    fi
done

for executable_path in mvnw compose/spark/smoke.sh; do
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

if ! command -v java >/dev/null 2>&1 || ! command -v jar >/dev/null 2>&1; then
    report_error "a JDK 17 or newer is required to build and inspect the Spark JAR"
fi

if ! command -v unzip >/dev/null 2>&1; then
    report_error "unzip is required to inspect the Spark JAR manifest"
fi

if [ "$failed" -ne 0 ]; then
    printf 'SPK-01 Spark static check failed.\n' >&2
    exit 1
fi

"$project_root/mvnw" --batch-mode --no-transfer-progress clean verify

if [ ! -f "$job_jar" ]; then
    report_error "Maven did not create spark/target/japan-earthquake-etl.jar"
else
    if ! jar tf "$job_jar" | grep -Fxq "$main_class_path"; then
        report_error "Spark JAR does not contain the HelloWorldJob main class"
    fi

    if jar tf "$job_jar" | grep -Eq '^org/apache/spark/'; then
        report_error "Spark dependencies must stay provided and must not be bundled in the job JAR"
    fi

    if ! unzip -p "$job_jar" META-INF/MANIFEST.MF \
        | grep -Fq 'Main-Class: vn.edu.uit.ie212.earthquake.spark.HelloWorldJob'; then
        report_error "Spark JAR manifest does not declare HelloWorldJob"
    fi
fi

docker compose --env-file "$env_file" -f "$compose_file" config --quiet
docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --quiet

services=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config --services)
rendered=$(docker compose --env-file "$env_file" -f "$compose_file" \
    --profile smoke config)

for service in spark-master spark-worker spark-client; do
    if ! printf '%s\n' "$services" | grep -Fxq "$service"; then
        report_error "missing Compose service: $service"
    fi
done

master_block=$(service_block "$rendered" spark-master)
worker_block=$(service_block "$rendered" spark-worker)
client_block=$(service_block "$rendered" spark-client)

for block in "$master_block" "$worker_block" "$client_block"; do
    require_fixed "$block" "image: $spark_image" \
        "every Spark service must use the reviewed SPK-01 image"
done

require_fixed "$master_block" "org.apache.spark.deploy.master.Master" \
    "spark-master must start the standalone Master process"
require_fixed "$master_block" "host_ip: 127.0.0.1" \
    "Spark master UI must bind to loopback"
require_fixed "$master_block" "target: 8080" \
    "Spark master UI must use container port 8080"
require_fixed "$master_block" "wget -q -O /dev/null http://localhost:8080/" \
    "spark-master must define an HTTP healthcheck"

require_fixed "$worker_block" "org.apache.spark.deploy.worker.Worker" \
    "spark-worker must start the standalone Worker process"
require_fixed "$worker_block" "condition: service_healthy" \
    "spark-worker must wait for a healthy master"
require_fixed "$worker_block" "source: pipeline_staging" \
    "spark-worker must mount pipeline_staging"
if printf '%s\n' "$worker_block" | grep -Fq "published:"; then
    report_error "Spark worker must not publish a host port"
fi

require_fixed "$client_block" "target: /opt/spark/smoke/run.sh" \
    "spark-client must mount the reviewed smoke runner"
require_fixed "$client_block" "condition: service_healthy" \
    "spark-client must wait for healthy master and worker services"
require_fixed "$client_block" "source: pipeline_staging" \
    "spark-client must mount pipeline_staging"
require_fixed "$client_block" "restart: \"no\"" \
    "spark-client must be a one-shot smoke service"

if ! grep -Fq 'FROM maven:3.9.16-eclipse-temurin-17 AS build' \
    "$project_root/compose/spark/Dockerfile"; then
    report_error "Spark image build must pin Maven 3.9.16 with Java 17"
fi

if ! grep -Fq 'FROM apache/spark:3.5.9-scala2.12-java17-ubuntu' \
    "$project_root/compose/spark/Dockerfile"; then
    report_error "Spark runtime must pin Spark 3.5.9, Scala 2.12 and Java 17"
fi

if ! grep -Fxq '**' "$project_root/.dockerignore"; then
    report_error "root Docker build context must deny files by default"
fi

if grep -Eq 'image:[[:space:]]+(apache/spark|japan-earthquake-etl/spark):(latest|edge)([[:space:]]|$)' \
    "$compose_file"; then
    report_error "Spark images must not use mutable latest/edge tags"
fi

if [ "$failed" -ne 0 ]; then
    printf 'SPK-01 Spark static check failed.\n' >&2
    exit 1
fi

printf 'SPK-01 Spark static check passed.\n'
