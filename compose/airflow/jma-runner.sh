#!/usr/bin/env sh
set -eu

runner_jar=/opt/pipeline/lib/usgs-ingest-runner.jar
if [ ! -r "$runner_jar" ]; then
    printf 'JMA runner JAR is missing or unreadable\n' >&2
    exit 2
fi
heap=${SOURCE_RUNNER_HEAP:-384m}
case "$heap" in
    128m|192m|256m|384m|512m) ;;
    *) printf 'INVALID_SOURCE_RUNNER_HEAP\n' >&2; exit 2 ;;
esac
exec "${JAVA_HOME:?JAVA_HOME is required}/bin/java" "-Xmx$heap" -cp "$runner_jar" \
    ie212.earthquake.spark.jma.JmaYearIngestRunner "$@"
