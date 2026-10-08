#!/usr/bin/env sh
set -eu
exec "${JAVA_HOME:?JAVA_HOME is required}/bin/java" -cp /opt/pipeline/lib/usgs-ingest-runner.jar \
    ie212.earthquake.spark.bronze.BronzeReuseVerifier "$@"
