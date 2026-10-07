#!/usr/bin/env sh
set -eu

runner_jar=/opt/pipeline/lib/usgs-ingest-runner.jar
if [ ! -r "$runner_jar" ]; then
    printf 'JMA runner JAR is missing or unreadable\n' >&2
    exit 2
fi
exec "${JAVA_HOME:?JAVA_HOME is required}/bin/java" -cp "$runner_jar" \
    ie212.earthquake.spark.jma.JmaYearIngestRunner "$@"
