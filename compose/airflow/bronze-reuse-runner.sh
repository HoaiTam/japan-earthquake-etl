#!/usr/bin/env sh
set -eu
heap=${SOURCE_RUNNER_HEAP:-384m}
case "$heap" in
    128m|192m|256m|384m|512m) ;;
    *) printf 'INVALID_SOURCE_RUNNER_HEAP\n' >&2; exit 2 ;;
esac
exec "${JAVA_HOME:?JAVA_HOME is required}/bin/java" "-Xmx$heap" -cp /opt/pipeline/lib/usgs-ingest-runner.jar \
    ie212.earthquake.spark.bronze.BronzeReuseVerifier "$@"
