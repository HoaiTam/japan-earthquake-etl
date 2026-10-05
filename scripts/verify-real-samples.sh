#!/usr/bin/env bash

set -euo pipefail

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
catalog="$project_root/tests/fixtures/real-samples/catalog.json"
compose_file=${COMPOSE_FILE:-"$project_root/compose.yaml"}
env_file=${ENV_FILE:-"$project_root/.env"}

"$project_root/scripts/check-real-sample-catalog.sh"

for command_name in jq docker; do
    command -v "$command_name" >/dev/null 2>&1 || {
        printf 'DAT-01 readback failed: missing command: %s\n' "$command_name" >&2
        exit 1
    }
done

catalog_value() {
    source_system=$1
    field=$2
    jq -er --arg source "$source_system" --arg field "$field" \
        '.samples[] | select(.source_system == $source) | .[$field]' "$catalog"
}

usgs_sample_id=$(catalog_value USGS sample_id)
usgs_manifest_uri=$(catalog_value USGS manifest_uri)
usgs_raw_uri=$(catalog_value USGS raw_object_uri)
usgs_sha256=$(catalog_value USGS sha256)
usgs_size=$(catalog_value USGS content_length_bytes)
usgs_count=$(catalog_value USGS expected_record_count)
jma_sample_id=$(catalog_value JMA_BULLETIN sample_id)
jma_staged_uri=$(catalog_value JMA_BULLETIN staged_object_uri)
jma_sha256=$(catalog_value JMA_BULLETIN sha256)
jma_size=$(catalog_value JMA_BULLETIN content_length_bytes)
jma_member=$(catalog_value JMA_BULLETIN member_name)
jma_count=$(catalog_value JMA_BULLETIN expected_record_count)
jma_record_length=$(catalog_value JMA_BULLETIN record_length_bytes)

docker_env=(
    -e "DAT01_USGS_SAMPLE_ID=$usgs_sample_id"
    -e "DAT01_USGS_MANIFEST_URI=$usgs_manifest_uri"
    -e "DAT01_USGS_RAW_URI=$usgs_raw_uri"
    -e "DAT01_USGS_SHA256=$usgs_sha256"
    -e "DAT01_USGS_SIZE=$usgs_size"
    -e "DAT01_USGS_COUNT=$usgs_count"
    -e "DAT01_JMA_SAMPLE_ID=$jma_sample_id"
    -e "DAT01_JMA_STAGED_URI=$jma_staged_uri"
    -e "DAT01_JMA_SHA256=$jma_sha256"
    -e "DAT01_JMA_SIZE=$jma_size"
    -e "DAT01_JMA_MEMBER=$jma_member"
    -e "DAT01_JMA_COUNT=$jma_count"
    -e "DAT01_JMA_RECORD_LENGTH=$jma_record_length"
)

if [ -n "${DAT01_AIRFLOW_CONTAINER:-}" ]; then
    runtime=(docker exec -i "${docker_env[@]}" "$DAT01_AIRFLOW_CONTAINER" python -)
else
    if [ ! -f "$env_file" ]; then
        printf 'DAT-01 readback requires ENV_FILE or DAT01_AIRFLOW_CONTAINER: %s\n' "$env_file" >&2
        exit 1
    fi
    runtime=(
        docker compose --env-file "$env_file" -f "$compose_file"
        exec -T "${docker_env[@]}" airflow-api-server python -
    )
fi

"${runtime[@]}" <<'PY'
import hashlib
import io
import json
import os
import zipfile
from urllib.parse import urlparse

import boto3
from botocore.config import Config


def required(name):
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"missing runtime value: {name}")
    return value


def parse_s3_uri(uri):
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.startswith("/"):
        raise SystemExit(f"invalid S3 URI in catalog: {uri}")
    if parsed.query or parsed.fragment:
        raise SystemExit("signed/query S3 URI is forbidden in DAT-01 catalog")
    return parsed.netloc, parsed.path[1:]


client = boto3.client(
    "s3",
    endpoint_url=required("MINIO_ENDPOINT"),
    aws_access_key_id=required("MINIO_ACCESS_KEY"),
    aws_secret_access_key=required("MINIO_SECRET_KEY"),
    region_name="us-east-1",
    config=Config(signature_version="s3v4"),
)


def read_object(uri):
    bucket, key = parse_s3_uri(uri)
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read()


usgs_manifest = json.loads(read_object(required("DAT01_USGS_MANIFEST_URI")))
usgs_raw = read_object(required("DAT01_USGS_RAW_URI"))
usgs_sha256 = required("DAT01_USGS_SHA256")
usgs_size = int(required("DAT01_USGS_SIZE"))
usgs_count = int(required("DAT01_USGS_COUNT"))

if usgs_manifest.get("bronze_status") != "BronzeReady":
    raise SystemExit("USGS manifest is not BronzeReady")
if usgs_manifest.get("raw_object_uri") != required("DAT01_USGS_RAW_URI"):
    raise SystemExit("USGS manifest raw_object_uri differs from catalog")
if usgs_manifest.get("sha256") != usgs_sha256:
    raise SystemExit("USGS manifest checksum differs from catalog")
if usgs_manifest.get("content_length_bytes") != usgs_size:
    raise SystemExit("USGS manifest content length differs from catalog")
if usgs_manifest.get("record_count_estimate") != usgs_count:
    raise SystemExit("USGS manifest record count differs from catalog")
if len(usgs_raw) != usgs_size or hashlib.sha256(usgs_raw).hexdigest() != usgs_sha256:
    raise SystemExit("USGS raw object readback differs from catalog")

jma_archive = read_object(required("DAT01_JMA_STAGED_URI"))
jma_sha256 = required("DAT01_JMA_SHA256")
jma_size = int(required("DAT01_JMA_SIZE"))
jma_member = required("DAT01_JMA_MEMBER")
jma_count = int(required("DAT01_JMA_COUNT"))
jma_record_length = int(required("DAT01_JMA_RECORD_LENGTH"))

if len(jma_archive) != jma_size or hashlib.sha256(jma_archive).hexdigest() != jma_sha256:
    raise SystemExit("JMA staged object readback differs from catalog")

with zipfile.ZipFile(io.BytesIO(jma_archive)) as archive:
    members = [item for item in archive.infolist() if not item.is_dir()]
    if len(members) != 1 or members[0].filename != jma_member:
        raise SystemExit("JMA ZIP member differs from catalog")
    records = archive.read(jma_member).splitlines()

if len(records) != jma_count:
    raise SystemExit("JMA record count differs from catalog")
if any(len(record) != jma_record_length for record in records):
    raise SystemExit("JMA staged archive contains a non-96-byte record")
if any(record[:1] not in {b"J", b"U", b"I"} for record in records):
    raise SystemExit("JMA staged archive contains an unexpected agency code")

print(
    "DAT-01 live readback passed: "
    f"usgs_sample={required('DAT01_USGS_SAMPLE_ID')} "
    f"usgs_bytes={len(usgs_raw)} usgs_records={usgs_count} "
    f"jma_sample={required('DAT01_JMA_SAMPLE_ID')} "
    f"jma_bytes={len(jma_archive)} jma_records={len(records)}"
)
PY
