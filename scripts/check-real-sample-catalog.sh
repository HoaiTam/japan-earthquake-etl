#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
catalog="$project_root/tests/fixtures/real-samples/catalog.json"
contract="$project_root/docs/specs/SHARED_REAL_SAMPLE_DATA.md"
jma_inventory="$project_root/config/jma/hypocenter_archives_v1.csv"
usgs_runbook="$project_root/docs/specs/USGS_LIVE_BRONZE_RUNBOOK.md"

fail() {
    printf 'DAT-01 real-sample catalog check failed: %s\n' "$1" >&2
    exit 1
}

for command_name in jq rg awk; do
    command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done

for required_file in "$catalog" "$contract" "$jma_inventory" "$usgs_runbook"; do
    [ -f "$required_file" ] || fail "missing ${required_file#"$project_root/"}"
done

jq -e '
    .catalog_version == "1.0"
    and .catalog_id == "dat01-real-samples-v1"
    and (.verified_at_utc | type == "string" and test("Z$"))
    and (.samples | type == "array" and length == 2)
    and (([.samples[].sample_id] | unique | length) == 2)
    and (all(.samples[];
        (.sample_id | type == "string" and length > 0)
        and (.source_url | type == "string" and startswith("https://"))
        and (.media_type | type == "string" and length > 0)
        and (.content_length_bytes | type == "number" and . > 0)
        and (.sha256 | type == "string" and test("^[0-9a-f]{64}$"))
        and (.expected_record_count | type == "number" and . >= 0)
        and (.retrieved_at_utc | type == "string" and test("Z$"))
        and (.validation | type == "object")
        and (.synthetic_fixture_hint | type == "string" and startswith("tests/fixtures/"))
    ))
    and (any(.samples[];
        .source_system == "USGS"
        and .source_kind == "event_api"
        and .sample_state == "BRONZE_READY"
        and .validation.bronze_status == "BronzeReady"
        and .validation.manifest_readback_verified == true
        and .validation.raw_readback_verified == true
        and .validation.checksum_verified == true
        and .manifest_uri == "s3://japan-earthquake/bronze/usgs/ingest_date=2023-01-01/run_id=usg06-live-20261004T121205Z-db580aece6dc/attempt=01/manifest.json"
        and (.raw_object_uri | startswith("s3://japan-earthquake/bronze/usgs/"))
        and .data_interval.window_start_utc == "2023-01-01T00:00:00Z"
        and .data_interval.window_end_utc == "2023-01-04T00:00:00Z"
        and .data_interval.interval_semantics == "[start,end)"
    ))
    and (any(.samples[];
        .source_system == "JMA_BULLETIN"
        and .source_kind == "annual_archive"
        and .sample_state == "STAGED_SOURCE"
        and .year == 2023
        and .segment == "full-year"
        and .archive_name == "h2023.zip"
        and .member_name == "h2023"
        and .catalog_release == "jma-lm-20251210T014153Z-sha256-e5ced2bf7275"
        and (.staged_object_uri | startswith("s3://japan-earthquake/bronze/_staging/jma/"))
        and .bronze_manifest_uri == null
        and .record_length_bytes == 96
        and .record_format == "jma-hypocenter-96-byte-v1"
        and .data_interval.native_timezone == "Asia/Tokyo"
        and .validation.zip_open_verified == true
        and .validation.staged_object_readback_verified == true
    ))
' "$catalog" >/dev/null || fail 'catalog.json does not match the DAT-01 schema/state contract'

usgs_run_id=$(jq -r '.samples[] | select(.source_system == "USGS") | .run_id' "$catalog")
usgs_sha256=$(jq -r '.samples[] | select(.source_system == "USGS") | .sha256' "$catalog")
usgs_count=$(jq -r '.samples[] | select(.source_system == "USGS") | .expected_record_count' "$catalog")
for marker in "$usgs_run_id" "$usgs_sha256" "| \`record_count_estimate\` | \`$usgs_count\` |"; do
    grep -F -- "$marker" "$usgs_runbook" >/dev/null || \
        fail "USGS catalog value is not backed by USG-06 runbook: $marker"
done

jma_year=$(jq -r '.samples[] | select(.source_system == "JMA_BULLETIN") | .year' "$catalog")
jma_url=$(jq -r '.samples[] | select(.source_system == "JMA_BULLETIN") | .source_url' "$catalog")
jma_archive=$(jq -r '.samples[] | select(.source_system == "JMA_BULLETIN") | .archive_name' "$catalog")
jma_member=$(jq -r '.samples[] | select(.source_system == "JMA_BULLETIN") | .member_name' "$catalog")
jma_last_modified=$(jq -r '.samples[] | select(.source_system == "JMA_BULLETIN") | .source_last_modified_utc' "$catalog")
jma_size=$(jq -r '.samples[] | select(.source_system == "JMA_BULLETIN") | .content_length_bytes' "$catalog")

inventory_match=$(awk -F, \
    -v year="$jma_year" \
    -v archive="$jma_archive" \
    -v member="$jma_member" \
    -v url="$jma_url" \
    -v modified="$jma_last_modified" \
    -v size="$jma_size" '
    NR > 1 && $2 == year && $6 == archive && $7 == member && $8 == url &&
        $10 == modified && $11 == size { matches++ }
    END { print matches + 0 }
' "$jma_inventory")
[ "$inventory_match" = '1' ] || fail 'JMA sample does not match exactly one JMA-01 inventory row'

for marker in \
    'catalog_id: "dat01-real-samples-v1"' \
    '`BRONZE_READY`' \
    '`STAGED_SOURCE`' \
    './scripts/check-real-sample-catalog.sh' \
    './scripts/verify-real-samples.sh'
do
    grep -F -- "$marker" "$contract" >/dev/null || fail "missing contract marker: $marker"
done

sample_file_count=$(find "$project_root/tests/fixtures/real-samples" -type f | wc -l | tr -d ' ')
[ "$sample_file_count" = '1' ] || fail 'real-samples directory must contain metadata catalog only'

if find "$project_root/tests/fixtures/real-samples" -type f \
    \( -name '*.zip' -o -name '*.geojson' -o -name '*.part' -o -name '*.bin' \) \
    | grep . >/dev/null; then
    fail 'raw sample payload must not be committed under tests/fixtures/real-samples'
fi

if rg -n -i \
    '(authorization|access[_-]?key|secret[_-]?key|bearer[[:space:]]|x-amz-(credential|signature)|/Users/|/home/[^/]+/|[A-Za-z]:\\\\Users\\\\)' \
    "$catalog" "$contract" >/dev/null; then
    fail 'catalog/docs contain a credential, signed URL or personal absolute path'
fi

printf 'DAT-01 real-sample catalog check passed: 2 samples, metadata only.\n'
