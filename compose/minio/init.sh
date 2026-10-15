#!/usr/bin/env sh

set -eu
umask 077

: "${MINIO_ENDPOINT:?MINIO_ENDPOINT is required}"
: "${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
: "${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
: "${MINIO_ACCESS_KEY:?MINIO_ACCESS_KEY is required}"
: "${MINIO_SECRET_KEY:?MINIO_SECRET_KEY is required}"
: "${DATA_BUCKET:?DATA_BUCKET is required}"
: "${BRONZE_PREFIX:?BRONZE_PREFIX is required}"
: "${SILVER_PREFIX:?SILVER_PREFIX is required}"
: "${WAREHOUSE_PATH:?WAREHOUSE_PATH is required}"

fail() {
    printf 'MinIO bootstrap failed: %s\n' "$1" >&2
    exit 1
}

validate_prefix() {
    prefix=$1
    label=$2

    case "$prefix" in
        ""|/*|*/|*//*|*[!A-Za-z0-9._/-]*)
            fail "$label must be a non-empty relative S3 prefix"
            ;;
    esac
}

if [ "${#DATA_BUCKET}" -lt 3 ] || [ "${#DATA_BUCKET}" -gt 63 ]; then
    fail "DATA_BUCKET must contain 3-63 characters"
fi

case "$DATA_BUCKET" in
    *[!a-z0-9.-]*|.*|-*|*.|*-|*..*|*.-*|*-.*)
        fail "DATA_BUCKET is not a valid local S3 bucket name"
        ;;
esac

warehouse_base="s3://$DATA_BUCKET/"
case "$WAREHOUSE_PATH" in
    "$warehouse_base"*) warehouse_prefix=${WAREHOUSE_PATH#"$warehouse_base"} ;;
    *) fail "WAREHOUSE_PATH must be inside DATA_BUCKET" ;;
esac

validate_prefix "$BRONZE_PREFIX" BRONZE_PREFIX
validate_prefix "$SILVER_PREFIX" SILVER_PREFIX
validate_prefix "$warehouse_prefix" WAREHOUSE_PATH

if [ "$BRONZE_PREFIX" = "$SILVER_PREFIX" ] ||
   [ "$BRONZE_PREFIX" = "$warehouse_prefix" ] ||
   [ "$SILVER_PREFIX" = "$warehouse_prefix" ]; then
    fail "Bronze, Silver and warehouse prefixes must be distinct"
fi

# The pinned mc supports alias JSON and user keys on stdin. Never put passwords
# in process arguments or echo CLI diagnostics that may include credentials.
json_escape() {
    printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}
printf '{"url":"%s","accessKey":"%s","secretKey":"%s","api":"S3v4","path":"auto"}\n' \
    "$(json_escape "$MINIO_ENDPOINT")" "$(json_escape "$MINIO_ROOT_USER")" \
    "$(json_escape "$MINIO_ROOT_PASSWORD")" | \
    mc alias import bootstrap >/dev/null 2>&1 || fail "admin alias import failed"
mc ready bootstrap >/dev/null 2>&1 || fail "cluster is not ready"
mc mb --ignore-existing "bootstrap/$DATA_BUCKET" >/dev/null 2>&1 || fail "bucket creation failed"

marker_file=/tmp/prefix-marker
: > "$marker_file"

for prefix in "$BRONZE_PREFIX" "$SILVER_PREFIX" "$warehouse_prefix"; do
    mc cp "$marker_file" "bootstrap/$DATA_BUCKET/$prefix/.keep" >/dev/null 2>&1 || fail "prefix marker creation failed"
done

policy_file=/tmp/pipeline-policy.json
cat > "$policy_file" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BucketMetadata",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucketMultipartUploads"
      ],
      "Resource": ["arn:aws:s3:::$DATA_BUCKET"]
    },
    {
      "Sid": "ListConfiguredPrefixes",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::$DATA_BUCKET"],
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "$BRONZE_PREFIX",
            "$BRONZE_PREFIX/*",
            "$SILVER_PREFIX",
            "$SILVER_PREFIX/*",
            "$warehouse_prefix",
            "$warehouse_prefix/*"
          ]
        }
      }
    },
    {
      "Sid": "BronzeAppendAndRead",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": ["arn:aws:s3:::$DATA_BUCKET/$BRONZE_PREFIX/*"]
    },
    {
      "Sid": "MutablePipelineLayers",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:AbortMultipartUpload",
        "s3:ListMultipartUploadParts"
      ],
      "Resource": [
        "arn:aws:s3:::$DATA_BUCKET/$SILVER_PREFIX/*",
        "arn:aws:s3:::$DATA_BUCKET/$warehouse_prefix/*"
      ]
    }
  ]
}
EOF

mc admin policy create bootstrap japan-earthquake-pipeline "$policy_file" \
    >/dev/null 2>&1 || fail "pipeline policy creation failed"
printf '%s\n%s\n' "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" | \
    mc admin user add bootstrap >/dev/null 2>&1 || fail "pipeline user creation failed"
mc admin policy attach bootstrap japan-earthquake-pipeline \
    --user "$MINIO_ACCESS_KEY" >/dev/null 2>&1 || fail "pipeline policy attachment failed"

printf 'MinIO bootstrap complete: bucket=%s prefixes=%s,%s,%s\n' \
    "$DATA_BUCKET" "$BRONZE_PREFIX" "$SILVER_PREFIX" "$warehouse_prefix"
