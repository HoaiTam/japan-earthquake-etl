#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
fixture_root="$project_root/tests/fixtures"
case_file="$fixture_root/cases.json"

fail() {
    printf 'CON-04 fixture check failed: %s\n' "$1" >&2
    exit 1
}

for command_name in jq unzip rg; do
    command -v "$command_name" >/dev/null 2>&1 || fail "missing command: $command_name"
done

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | awk '{print $1}'
    else
        fail 'missing sha256sum or shasum'
    fi
}

[ -f "$case_file" ] || fail 'missing cases.json'
jq -e '
    .matrix_version == "1.0"
    and (.cases | type == "array" and length >= 17)
    and (all(.cases[];
        (.case_id | type == "string" and length > 0)
        and (.source_system | IN("USGS", "JMA_BULLETIN", "MULTI_SOURCE"))
        and (.category | type == "string" and length > 0)
        and (.input_paths | type == "array" and length > 0)
        and (.expected | type == "object")
        and (.expected | has("bronze_status"))
        and (.expected | has("parsed_count"))
        and (.expected | has("valid_count"))
        and (.expected | has("rejected_count"))
        and (.expected | has("current_count"))
        and (.expected | has("canonical_count"))
        and (.expected.reason_codes | type == "array")
        and (.expected.assertions | type == "array" and length > 0)
    ))
' "$case_file" >/dev/null || fail 'cases.json does not match the fixture matrix contract'

case_count=$(jq -r '.cases[].case_id' "$case_file" | wc -l | tr -d ' ')
unique_case_count=$(jq -r '.cases[].case_id' "$case_file" | sort -u | wc -l | tr -d ' ')
[ "$case_count" = "$unique_case_count" ] || fail 'case_id values must be unique'

for source_system in USGS JMA_BULLETIN; do
    for category in success empty invalid duplicate revised timezone checksum_mismatch; do
        jq -e --arg source "$source_system" --arg category "$category" \
            'any(.cases[]; .source_system == $source and .category == $category)' \
            "$case_file" >/dev/null || fail "missing $source_system/$category case"
    done
done

jq -r '.cases[].input_paths[]' "$case_file" | while IFS= read -r relative_path; do
    [ -f "$fixture_root/$relative_path" ] || fail "missing input path: $relative_path"
done

valid_geojson='success.geojson empty.geojson invalid-fields.geojson duplicate.geojson revision-v1.geojson revision-v2.geojson timezone-boundary.geojson ambiguous.geojson'
for file_name in $valid_geojson; do
    jq -e '
        .type == "FeatureCollection"
        and (.features | type == "array")
        and (.metadata.count == (.features | length))
    ' "$fixture_root/usgs/$file_name" >/dev/null || fail "invalid GeoJSON structure: usgs/$file_name"
done

if jq -e . "$fixture_root/usgs/invalid-json.geojson" >/dev/null 2>&1; then
    fail 'invalid-json.geojson must stay intentionally malformed'
fi

duplicate_ids=$(jq -r '.features[].id' "$fixture_root/usgs/duplicate.geojson" | sort -u | wc -l | tr -d ' ')
[ "$duplicate_ids" = '1' ] || fail 'USGS duplicate fixture must contain one repeated source id'

revision_v1_id=$(jq -r '.features[0].id' "$fixture_root/usgs/revision-v1.geojson")
revision_v2_id=$(jq -r '.features[0].id' "$fixture_root/usgs/revision-v2.geojson")
[ "$revision_v1_id" = "$revision_v2_id" ] || fail 'USGS revision fixtures must share source id'
revision_v1_updated=$(jq -r '.features[0].properties.updated' "$fixture_root/usgs/revision-v1.geojson")
revision_v2_updated=$(jq -r '.features[0].properties.updated' "$fixture_root/usgs/revision-v2.geojson")
[ "$revision_v2_updated" -gt "$revision_v1_updated" ] || fail 'USGS v2 updated must be newer than v1'

fixed_width_files='success.hyp duplicate.hyp revision-v1.hyp revision-v2.hyp timezone-boundary.hyp ambiguous.hyp'
for file_name in $fixed_width_files; do
    LC_ALL=C awk '
        length($0) != 96 { exit 1 }
        END { if (NR == 0) exit 1 }
    ' "$fixture_root/jma/fixed-width/$file_name" || fail "JMA record is not 96 bytes: $file_name"
done

[ ! -s "$fixture_root/jma/fixed-width/empty.hyp" ] || fail 'JMA empty fixture must be zero bytes'
invalid_length=$(LC_ALL=C awk 'NR == 1 { print length($0) }' "$fixture_root/jma/fixed-width/invalid-record-length.hyp")
[ "$invalid_length" = '95' ] || fail 'JMA invalid-record-length fixture must contain a 95-byte record'

jma_field() {
    file_name=$1
    line_number=$2
    range=$3
    sed -n "${line_number}p" "$fixture_root/jma/fixed-width/$file_name" | cut -c "$range"
}

[ "$(jma_field success.hyp 1 1)" = 'J' ] || fail 'JMA agency column mismatch'
[ "$(jma_field success.hyp 1 2-17)" = '2023090112345678' ] || fail 'JMA success origin columns mismatch'
[ "$(jma_field success.hyp 1 22-28)" = '0354020' ] || fail 'JMA success latitude columns mismatch'
[ "$(jma_field success.hyp 1 33-40)" = '01394560' ] || fail 'JMA success longitude columns mismatch'
[ "$(jma_field success.hyp 1 45-49)" = '01000' ] || fail 'JMA success depth columns mismatch'
[ "$(jma_field success.hyp 1 53-55)" = '52J' ] || fail 'JMA success magnitude columns mismatch'
[ "$(jma_field success.hyp 1 61)" = '1' ] || fail 'JMA natural event flag mismatch'
[ "$(jma_field success.hyp 2 61)" = '3' ] || fail 'JMA artificial event flag mismatch'
[ "$(jma_field success.hyp 1 96)" = 'K' ] || fail 'JMA determination flag mismatch'
[ "$(jma_field timezone-boundary.hyp 1 2-17)" = '2023123123590000' ] || fail 'JMA year-end JST fixture mismatch'
[ "$(jma_field timezone-boundary.hyp 2 2-17)" = '1997093023595900' ] || fail 'JMA legacy boundary fixture mismatch'
[ "$(jma_field timezone-boundary.hyp 3 2-17)" = '1997100100000000' ] || fail 'JMA unified boundary fixture mismatch'
[ "$(jma_field duplicate.hyp 1 1-96)" = "$(jma_field duplicate.hyp 2 1-96)" ] || fail 'JMA duplicate records must be byte-identical'
[ "$(jma_field revision-v1.hyp 1 1-44)" = "$(jma_field revision-v2.hyp 1 1-44)" ] || fail 'JMA revisions must share origin and coordinates'
[ "$(jma_field revision-v2.hyp 1 53-55)" = '42J' ] || fail 'JMA revision v2 magnitude mismatch'

temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/con04-check.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

archive_pairs='success.zip:success.hyp
empty.zip:empty.hyp
invalid-record-length.zip:invalid-record-length.hyp
duplicate.zip:duplicate.hyp
revision-v1.zip:revision-v1.hyp
revision-v2.zip:revision-v2.hyp
timezone-boundary.zip:timezone-boundary.hyp
ambiguous.zip:ambiguous.hyp'

for pair in $archive_pairs; do
    archive_name=${pair%%:*}
    raw_name=${pair#*:}
    archive_path="$fixture_root/jma/archives/$archive_name"
    unzip -tqq "$archive_path" >/dev/null || fail "invalid ZIP: $archive_name"
    entries=$(unzip -Z1 "$archive_path")
    [ "$entries" = 'hypo.dat' ] || fail "ZIP must contain only hypo.dat: $archive_name"
    extracted="$temporary_root/$raw_name"
    unzip -p "$archive_path" hypo.dat > "$extracted"
    cmp -s "$fixture_root/jma/fixed-width/$raw_name" "$extracted" || fail "ZIP content mismatch: $archive_name"
done

while IFS='  ' read -r expected_digest relative_path; do
    [ -n "$expected_digest" ] || continue
    relative_path=$(printf '%s' "$relative_path" | sed 's/^ *//')
    actual_digest=$(sha256_file "$fixture_root/$relative_path")
    [ "$actual_digest" = "$expected_digest" ] || fail "checksum drift: $relative_path"
done < "$fixture_root/SHA256SUMS"

checksum_count=$(wc -l < "$fixture_root/SHA256SUMS" | tr -d ' ')
[ "$checksum_count" = '25' ] || fail 'SHA256SUMS must cover exactly 25 raw fixture files'

for mismatch_file in "$fixture_root/usgs/checksum-mismatch.sha256" "$fixture_root/jma/checksum-mismatch.sha256"; do
    expected_digest=$(awk '{print $1}' "$mismatch_file")
    relative_path=$(awk '{print $2}' "$mismatch_file")
    actual_digest=$(sha256_file "$fixture_root/$relative_path")
    [ "$actual_digest" != "$expected_digest" ] || fail "checksum mismatch fixture unexpectedly matches: $relative_path"
done

if rg -n -i 'authorization|access[_-]?key|secret[_-]?key|bearer[[:space:]]' "$fixture_root" >/dev/null; then
    fail 'fixture contains a credential-like field'
fi

printf 'CON-04 shared fixture check passed: %s cases.\n' "$case_count"
