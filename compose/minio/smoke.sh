#!/usr/bin/env sh

set -eu
umask 077

fail() {
    printf 'MIO-01 smoke test failed: %s\n' "$1" >&2
    exit 1
}

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

json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}
printf '{"url":"%s","accessKey":"%s","secretKey":"%s","api":"S3v4","path":"auto"}\n' \
    "$(json_escape "$MINIO_ENDPOINT")" "$(json_escape "$MINIO_ACCESS_KEY")" \
    "$(json_escape "$MINIO_SECRET_KEY")" | \
    mc alias import pipeline >/dev/null 2>&1 || fail "pipeline alias import failed"
mc ready pipeline >/dev/null 2>&1 || fail "cluster is not ready"

for prefix in "$BRONZE_PREFIX" "$SILVER_PREFIX" "$warehouse_prefix"; do
    mc stat "pipeline/$DATA_BUCKET/$prefix/.keep" >/dev/null 2>&1 || fail "prefix marker check failed"
done

smoke_object="pipeline/$DATA_BUCKET/$SILVER_PREFIX/_smoke/mio-01-${HOSTNAME:-container}-$$.txt"
expected="mio-01-write-read-${HOSTNAME:-container}-$$"

cleanup() {
    mc rm --force "$smoke_object" >/dev/null 2>&1 || true
}
trap cleanup 0 HUP INT TERM

printf '%s' "$expected" | mc pipe "$smoke_object" >/dev/null 2>&1 || fail "probe write failed"
actual=$(mc cat "$smoke_object" 2>/dev/null) || fail "probe read failed"

if [ "$actual" != "$expected" ]; then
    printf 'MIO-01 smoke test failed: object content mismatch\n' >&2
    exit 1
fi

mc stat "$smoke_object" >/dev/null 2>&1 || fail "probe stat failed"
cleanup
trap - 0 HUP INT TERM

printf 'MIO-01 MinIO write/read smoke test passed.\n'
