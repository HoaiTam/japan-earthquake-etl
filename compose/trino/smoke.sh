#!/usr/bin/env bash

set -euo pipefail

server_url="http://${TRINO_HOST}:${TRINO_INTERNAL_PORT}"
qualified_schema="${TRINO_CATALOG}.${TRINO_SCHEMA}"
smoke_table="${qualified_schema}.qry_01_smoke"

run_query() {
    /usr/bin/trino \
        --server "$server_url" \
        --user qry-01-smoke \
        --output-format TSV \
        --execute "$1"
}

cleanup() {
    run_query "DROP TABLE IF EXISTS ${smoke_table}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

catalogs=$(run_query "SHOW CATALOGS")
if ! printf '%s\n' "$catalogs" | grep -Fxq "$TRINO_CATALOG"; then
    printf 'ERROR: Trino catalog is not visible: %s\n' "$TRINO_CATALOG" >&2
    exit 1
fi

run_query "CREATE SCHEMA IF NOT EXISTS ${qualified_schema}" >/dev/null

schemas=$(run_query "SHOW SCHEMAS FROM ${TRINO_CATALOG}")
if ! printf '%s\n' "$schemas" | grep -Fxq "$TRINO_SCHEMA"; then
    printf 'ERROR: Trino schema is not visible: %s\n' "$qualified_schema" >&2
    exit 1
fi

run_query "DROP TABLE IF EXISTS ${smoke_table}" >/dev/null
run_query "CREATE TABLE ${smoke_table} (event_id varchar, magnitude double) WITH (format = 'PARQUET')" >/dev/null
run_query "INSERT INTO ${smoke_table} VALUES ('qry-01', 6.5)" >/dev/null

result=$(run_query "SELECT count(*), max(event_id), CAST(sum(magnitude) AS decimal(3,1)) FROM ${smoke_table}")
IFS=$'\t' read -r row_count event_id magnitude <<< "$result"

if [ "$row_count" != "1" ] || [ "$event_id" != "qry-01" ] || [ "$magnitude" != "6.5" ]; then
    printf 'ERROR: Iceberg smoke result mismatch (count=%s event_id=%s magnitude=%s).\n' \
        "$row_count" "$event_id" "$magnitude" >&2
    exit 1
fi

run_query "DROP TABLE ${smoke_table}" >/dev/null
trap - EXIT

printf '%s\n' \
    "QRY-01 query smoke passed: catalog=${TRINO_CATALOG} schema=${TRINO_SCHEMA} row_count=1."
