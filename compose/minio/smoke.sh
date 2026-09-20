#!/usr/bin/env sh

set -eu

: "${MINIO_ENDPOINT:?MINIO_ENDPOINT is required}"
: "${MINIO_ACCESS_KEY:?MINIO_ACCESS_KEY is required}"
: "${MINIO_SECRET_KEY:?MINIO_SECRET_KEY is required}"
: "${DATA_BUCKET:?DATA_BUCKET is required}"
: "${BRONZE_PREFIX:?BRONZE_PREFIX is required}"
: "${SILVER_PREFIX:?SILVER_PREFIX is required}"
: "${WAREHOUSE_PATH:?WAREHOUSE_PATH is required}"

warehouse_base="s3://$DATA_BUCKET/"
case "$WAREHOUSE_PATH" in
    "$warehouse_base"*) warehouse_prefix=${WAREHOUSE_PATH#"$warehouse_base"} ;;
    *)
        printf 'MIO-01 smoke test failed: WAREHOUSE_PATH is outside DATA_BUCKET\n' >&2
        exit 1
        ;;
esac

mc alias set pipeline "$MINIO_ENDPOINT" "$MINIO_ACCESS_KEY" \
    "$MINIO_SECRET_KEY" >/dev/null
mc ready pipeline >/dev/null

for prefix in "$BRONZE_PREFIX" "$SILVER_PREFIX" "$warehouse_prefix"; do
    mc stat "pipeline/$DATA_BUCKET/$prefix/.keep" >/dev/null
done

smoke_object="pipeline/$DATA_BUCKET/$SILVER_PREFIX/_smoke/mio-01-${HOSTNAME:-container}-$$.txt"
expected="mio-01-write-read-${HOSTNAME:-container}-$$"

cleanup() {
    mc rm --force "$smoke_object" >/dev/null 2>&1 || true
}
trap cleanup 0 HUP INT TERM

printf '%s' "$expected" | mc pipe "$smoke_object" >/dev/null
actual=$(mc cat "$smoke_object")

if [ "$actual" != "$expected" ]; then
    printf 'MIO-01 smoke test failed: object content mismatch\n' >&2
    exit 1
fi

mc stat "$smoke_object" >/dev/null
cleanup
trap - 0 HUP INT TERM

printf 'MIO-01 MinIO write/read smoke test passed.\n'
