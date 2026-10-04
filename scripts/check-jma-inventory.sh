#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
inventory="$project_root/config/jma/hypocenter_archives_v1.csv"
contract="$project_root/docs/specs/JMA_ARCHIVE_INVENTORY.md"
mode=${1:-offline}

case "$mode" in
    offline) ;;
    --live) ;;
    *)
        printf 'Usage: %s [--live]\n' "$0" >&2
        exit 2
        ;;
esac

fail() {
    printf 'JMA-01 inventory check failed: %s\n' "$1" >&2
    exit 1
}

[ -f "$inventory" ] || fail "missing config/jma/hypocenter_archives_v1.csv"
[ -f "$contract" ] || fail "missing docs/specs/JMA_ARCHIVE_INVENTORY.md"

expected_header='inventory_version,year,segment,native_start_jst,native_end_jst,archive_name,member_name,source_url,observed_release_hint,observed_last_modified_utc,observed_content_length_bytes,media_type,record_format,catalog_era'
actual_header=$(sed -n '1p' "$inventory")
[ "$actual_header" = "$expected_header" ] || fail 'unexpected CSV header'

awk -F, '
function reject(message) {
    printf "JMA-01 inventory check failed at CSV line %d: %s\n", NR, message > "/dev/stderr"
    invalid = 1
}

NR == 1 { next }

{
    if (NF != 14) {
        reject("expected 14 columns")
        next
    }

    version = $1
    year = $2 + 0
    segment = $3
    start = $4
    end = $5
    archive = $6
    member = $7
    url = $8
    release_hint = $9
    last_modified = $10
    content_length = $11
    media_type = $12
    record_format = $13
    era = $14

    rows++
    years[year]++
    if (seen_archive[archive]++) reject("duplicate archive_name " archive)
    if (rows == 1 && start != "1984-01-01T00:00:00+09:00") {
        reject("inventory must start at the 1984 JST boundary")
    } else if (rows > 1 && start != previous_end) {
        reject("inventory has a gap, overlap or incorrect row order")
    }
    previous_end = end

    if (version != "1.0") reject("inventory_version must be 1.0")
    if (year < 1984 || year > 2023) reject("year outside 1984-2023")

    if (year == 1997 && segment == "jan-sep") {
        expected_start = "1997-01-01T00:00:00+09:00"
        expected_end = "1997-10-01T00:00:00+09:00"
        expected_archive = "h199701.zip"
        expected_member = "h199701"
        expected_era = "LEGACY"
    } else if (year == 1997 && segment == "oct-dec") {
        expected_start = "1997-10-01T00:00:00+09:00"
        expected_end = "1998-01-01T00:00:00+09:00"
        expected_archive = "h199710.zip"
        expected_member = "h199710"
        expected_era = "UNIFIED"
    } else {
        if (segment != "full-year") reject("non-1997 row must use full-year")
        expected_start = sprintf("%04d-01-01T00:00:00+09:00", year)
        expected_end = sprintf("%04d-01-01T00:00:00+09:00", year + 1)
        expected_archive = sprintf("h%04d.zip", year)
        expected_member = sprintf("h%04d", year)
        expected_era = year <= 1996 ? "LEGACY" : "UNIFIED"
    }

    expected_url = "https://www.data.jma.go.jp/eqev/data/bulletin/data/hypo/" expected_archive
    if (start != expected_start || end != expected_end) reject("invalid native JST interval")
    if (archive != expected_archive || member != expected_member) reject("invalid archive/member name")
    if (url != expected_url) reject("invalid source URL")
    if (length(release_hint) != 19 || release_hint !~ /^lm-[0-9]+T[0-9]+Z$/) reject("invalid release hint")
    if (length(last_modified) != 20 || last_modified !~ /^[0-9-]+T[0-9:]+Z$/) reject("invalid Last-Modified UTC value")
    release_from_modified = last_modified
    gsub(/[-:]/, "", release_from_modified)
    if (release_hint != "lm-" release_from_modified) reject("release hint and Last-Modified disagree")
    if (content_length !~ /^[0-9]+$/ || content_length + 0 <= 0) reject("content length must be positive")
    if (media_type != "application/zip") reject("media type must be application/zip")
    if (record_format != "jma-hypocenter-96-byte-v1") reject("unexpected record format")
    if (era != expected_era) reject("incorrect catalog era")
}

END {
    if (rows != 41) {
        printf "JMA-01 inventory check failed: expected 41 archive rows, got %d\n", rows > "/dev/stderr"
        invalid = 1
    }
    if (previous_end != "2024-01-01T00:00:00+09:00") {
        printf "JMA-01 inventory check failed: inventory must end at the 2024 JST boundary\n" > "/dev/stderr"
        invalid = 1
    }
    for (year = 1984; year <= 2023; year++) {
        expected = year == 1997 ? 2 : 1
        if (years[year] != expected) {
            printf "JMA-01 inventory check failed: year %d expected %d row(s), got %d\n", year, expected, years[year] > "/dev/stderr"
            invalid = 1
        }
    }
    if (invalid) exit 1
}
' "$inventory"

for marker in \
    'calendar_years: 40' \
    'archive_entries: 41' \
    'record_length_bytes: 96' \
    'native_timezone: "Asia/Tokyo"' \
    'geodetic_datum: "Japanese Geodetic Datum 2000"' \
    'jma-lm-<last-modified-UTC-basic>-sha256-<12-hex-first>' \
    'Source: Japan Meteorological Agency (JMA)'
do
    grep -F -- "$marker" "$contract" >/dev/null || fail "missing contract marker: $marker"
done

if [ "$mode" = "--live" ]; then
    command -v curl >/dev/null 2>&1 || fail 'curl is required for --live'
    headers_file=$(mktemp "${TMPDIR:-/tmp}/jma01-headers.XXXXXX")
    trap 'rm -f "$headers_file"' EXIT HUP INT TERM

    tail -n +2 "$inventory" | while IFS=, read -r \
        inventory_version year segment native_start native_end archive member \
        source_url expected_release_hint expected_last_modified expected_length \
        expected_media_type record_format catalog_era
    do
        : "$inventory_version" "$year" "$segment" "$native_start" "$native_end" \
            "$member" "$expected_last_modified" "$record_format" "$catalog_era"
        curl -fsSIL --max-time 30 --retry 2 "$source_url" >"$headers_file" || \
            fail "HEAD request failed for $archive"

        actual_media_type=$(awk '
            BEGIN { IGNORECASE=1; value="" }
            /^content-type:/ {
                sub(/^[^:]+:[[:space:]]*/, "")
                sub(/;.*/, "")
                gsub(/\r/, "")
                value=$0
            }
            END { print value }
        ' "$headers_file")
        actual_length=$(awk '
            BEGIN { IGNORECASE=1; value="" }
            /^content-length:/ {
                sub(/^[^:]+:[[:space:]]*/, "")
                gsub(/\r/, "")
                value=$0
            }
            END { print value }
        ' "$headers_file")
        actual_release_hint=$(awk '
            BEGIN {
                IGNORECASE=1
                month["Jan"]="01"; month["Feb"]="02"; month["Mar"]="03"
                month["Apr"]="04"; month["May"]="05"; month["Jun"]="06"
                month["Jul"]="07"; month["Aug"]="08"; month["Sep"]="09"
                month["Oct"]="10"; month["Nov"]="11"; month["Dec"]="12"
                value=""
            }
            /^last-modified:/ {
                sub(/^[^:]+:[[:space:]]*/, "")
                gsub(/\r/, "")
                split($0, part, /[[:space:]]+/)
                time=part[5]
                gsub(/:/, "", time)
                value=sprintf("lm-%s%s%02dT%sZ", part[4], month[part[3]], part[2], time)
            }
            END { print value }
        ' "$headers_file")

        [ "$actual_media_type" = "$expected_media_type" ] || \
            fail "$archive media type drifted: $actual_media_type"
        [ "$actual_length" = "$expected_length" ] || \
            fail "$archive content length drifted: expected $expected_length, got $actual_length"
        [ "$actual_release_hint" = "$expected_release_hint" ] || \
            fail "$archive Last-Modified drifted: expected $expected_release_hint, got $actual_release_hint"
    done
fi

printf 'JMA-01 inventory check passed: 40 years, 41 archive entries, mode=%s.\n' "$mode"
