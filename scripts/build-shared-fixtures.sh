#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
fixture_root="$project_root/tests/fixtures"
jma_root="$fixture_root/jma"
fixed_width_root="$jma_root/fixed-width"
archive_root="$jma_root/archives"

command -v zip >/dev/null 2>&1 || {
    printf '%s\n' 'Missing required command: zip' >&2
    exit 1
}

mkdir -p "$fixed_width_root" "$archive_root"

temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/con04-fixtures.XXXXXX")
trap 'rm -rf "$temporary_root"' EXIT HUP INT TERM

format_record() {
    # JMA hypocenter record: exactly 96 ASCII bytes, without the line ending.
    # Arguments follow official columns 01..96 in order.
    printf '%1.1s%4.4s%2.2s%2.2s%2.2s%2.2s%4.4s%4.4s%3.3s%4.4s%4.4s%4.4s%4.4s%4.4s%5.5s%3.3s%2.2s%1.1s%2.2s%1.1s%1.1s%1.1s%1.1s%1.1s%1.1s%1.1s%1.1s%3.3s%-24.24s%3.3s%1.1s' \
        "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" \
        "${11}" "${12}" "${13}" "${14}" "${15}" "${16}" "${17}" \
        "${18}" "${19}" "${20}" "${21}" "${22}" "${23}" "${24}" \
        "${25}" "${26}" "${27}" "${28}" "${29}" "${30}" "${31}"
}

write_record() {
    format_record "$@"
    printf '\n'
}

{
    write_record J 2023 09 01 12 34 5678 '' 035 4020 '' 0139 4560 '' 01000 '' 52 J '' '' 7 1 1 B '' '' 3 123 'TOKYO BAY' 042 K
    write_record J 2023 09 01 13 00 0000 '' 036 0000 '' 0140 0000 '' 00500 '' 21 J '' '' 7 1 3 '' '' '' 3 124 'SYNTHETIC QUARRY' 010 A
} > "$fixed_width_root/success.hyp"

: > "$fixed_width_root/empty.hyp"

invalid_record=$(format_record J 2023 09 02 01 02 0300 '' 035 3000 '' 0139 3000 '' 02000 '' 35 J '' '' 7 1 1 2 '' '' 3 125 'TRUNCATED RECORD' 015 K)
printf '%s\n' "${invalid_record%?}" > "$fixed_width_root/invalid-record-length.hyp"

{
    write_record J 2023 09 03 04 05 0600 '' 038 1200 '' 0142 0600 '' 03500 '' 46 J '' '' 7 1 1 3 '' '' 2 126 'DUPLICATE EVENT' 025 K
    write_record J 2023 09 03 04 05 0600 '' 038 1200 '' 0142 0600 '' 03500 '' 46 J '' '' 7 1 1 3 '' '' 2 126 'DUPLICATE EVENT' 025 K
} > "$fixed_width_root/duplicate.hyp"

write_record J 2023 10 05 03 02 0125 '' 036 3000 '' 0140 1500 '' 02500 '' 40 J '' '' 7 1 1 3 '' '' 3 127 'REVISION EVENT' 020 A > "$fixed_width_root/revision-v1.hyp"
write_record J 2023 10 05 03 02 0125 '' 036 3000 '' 0140 1500 '' 02400 '' 42 J '' '' 7 1 1 3 '' '' 3 127 'REVISION EVENT' 028 K > "$fixed_width_root/revision-v2.hyp"

{
    write_record J 2023 12 31 23 59 0000 '' 035 0000 '' 0139 0000 '' 01000 '' 30 J '' '' 7 1 1 2 '' '' 3 128 'YEAR END JST' 012 K
    write_record J 1997 09 30 23 59 5900 '' 035 1000 '' 0139 1000 '' 01200 '' 31 J '' '' 5 1 1 2 '' '' 3 129 'LEGACY EDGE' 014 K
    write_record J 1997 10 01 00 00 0000 '' 035 2000 '' 0139 2000 '' 01300 '' 32 J '' '' 5 1 1 2 '' '' 3 130 'UNIFIED EDGE' 016 K
} > "$fixed_width_root/timezone-boundary.hyp"

{
    write_record J 2023 08 15 10 00 0000 '' 035 4000 '' 0139 4500 '' 01000 '' 51 J '' '' 7 1 1 3 '' '' 3 131 'AMBIGUOUS WEST' 030 K
    write_record J 2023 08 15 10 00 0400 '' 035 4010 '' 0139 4510 '' 01100 '' 51 J '' '' 7 1 1 3 '' '' 3 132 'AMBIGUOUS EAST' 031 K
} > "$fixed_width_root/ambiguous.hyp"

build_archive() {
    source_file=$1
    target_file=$2
    stage="$temporary_root/archive-stage"
    archive="$temporary_root/archive.zip"

    mkdir -p "$stage"
    cp "$source_file" "$stage/hypo.dat"
    touch -t 200001010000 "$stage/hypo.dat"
    (
        cd "$stage"
        zip -X -q "$archive" hypo.dat
    )
    mv "$archive" "$target_file"
    rm -f "$stage/hypo.dat"
    rmdir "$stage"
}

build_archive "$fixed_width_root/success.hyp" "$archive_root/success.zip"
build_archive "$fixed_width_root/empty.hyp" "$archive_root/empty.zip"
build_archive "$fixed_width_root/invalid-record-length.hyp" "$archive_root/invalid-record-length.zip"
build_archive "$fixed_width_root/duplicate.hyp" "$archive_root/duplicate.zip"
build_archive "$fixed_width_root/revision-v1.hyp" "$archive_root/revision-v1.zip"
build_archive "$fixed_width_root/revision-v2.hyp" "$archive_root/revision-v2.zip"
build_archive "$fixed_width_root/timezone-boundary.hyp" "$archive_root/timezone-boundary.zip"
build_archive "$fixed_width_root/ambiguous.hyp" "$archive_root/ambiguous.zip"

sha256_file() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

fixture_paths='usgs/success.geojson
usgs/empty.geojson
usgs/invalid-fields.geojson
usgs/invalid-json.geojson
usgs/duplicate.geojson
usgs/revision-v1.geojson
usgs/revision-v2.geojson
usgs/timezone-boundary.geojson
usgs/ambiguous.geojson
jma/fixed-width/success.hyp
jma/fixed-width/empty.hyp
jma/fixed-width/invalid-record-length.hyp
jma/fixed-width/duplicate.hyp
jma/fixed-width/revision-v1.hyp
jma/fixed-width/revision-v2.hyp
jma/fixed-width/timezone-boundary.hyp
jma/fixed-width/ambiguous.hyp
jma/archives/success.zip
jma/archives/empty.zip
jma/archives/invalid-record-length.zip
jma/archives/duplicate.zip
jma/archives/revision-v1.zip
jma/archives/revision-v2.zip
jma/archives/timezone-boundary.zip
jma/archives/ambiguous.zip'

checksum_file="$temporary_root/SHA256SUMS"
: > "$checksum_file"
for relative_path in $fixture_paths; do
    digest=$(sha256_file "$fixture_root/$relative_path")
    printf '%s  %s\n' "$digest" "$relative_path" >> "$checksum_file"
done
mv "$checksum_file" "$fixture_root/SHA256SUMS"

zero_digest='0000000000000000000000000000000000000000000000000000000000000000'
printf '%s  %s\n' "$zero_digest" 'usgs/success.geojson' > "$fixture_root/usgs/checksum-mismatch.sha256"
printf '%s  %s\n' "$zero_digest" 'jma/archives/success.zip' > "$fixture_root/jma/checksum-mismatch.sha256"

printf '%s\n' 'CON-04 shared fixtures regenerated.'
