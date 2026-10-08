#!/usr/bin/env sh

# Check the repository's explicit Dockerfile/allowlist convention, not arbitrary
# Dockerfile or dockerignore syntax. The real Docker build remains the final gate.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root="$script_dir/.."
if [ "$#" -gt 1 ]; then
    printf 'Usage: %s [project-root]\n' "$0" >&2
    exit 2
fi
if [ "$#" -eq 1 ]; then
    project_root=$1
fi

failed=0
report_error() {
    printf 'ERROR: %s\n' "$1" >&2
    failed=1
}

for relative_path in \
    pom.xml \
    spark/pom.xml \
    tests/fixtures/cases.json \
    tests/fixtures/usgs/success.geojson \
    tests/fixtures/jma/archives/success.zip \
    config/jma/hypocenter_archives_v1.csv
do
    if [ ! -f "$project_root/$relative_path" ]; then
        report_error "missing Java build input: $relative_path"
    fi
done

ignore_file="$project_root/.dockerignore"
if [ ! -f "$ignore_file" ]; then
    report_error "missing .dockerignore"
else
    for rule in \
        '**' '!pom.xml' '!spark/' '!spark/pom.xml' '!spark/src/' '!spark/src/**' \
        '!tests/' '!tests/fixtures/' '!tests/fixtures/**' \
        '!config/' '!config/jma/' '!config/jma/hypocenter_archives_v1.csv'
    do
        if ! grep -Fxq "$rule" "$ignore_file"; then
            report_error "missing Java build context rule: $rule"
        fi
    done
fi

for relative_path in compose/spark/Dockerfile compose/airflow/Dockerfile; do
    dockerfile="$project_root/$relative_path"
    if [ ! -f "$dockerfile" ]; then
        report_error "missing Java builder Dockerfile: $relative_path"
        continue
    fi

    if ! awk '
        {
            line = $0
            sub(/^[[:space:]]+/, "", line)
            sub(/[[:space:]]+$/, "", line)
        }
        line ~ /^FROM[[:space:]]/ {
            in_build = (line ~ /[[:space:]]AS[[:space:]]build$/)
        }
        in_build && line == "COPY tests/fixtures ./tests/fixtures" { fixtures = 1 }
        in_build && line == "COPY config/jma ./config/jma" { inventory = 1 }
        in_build && line ~ /^RUN[[:space:]]+mvn[[:space:]].*clean[[:space:]]+verify$/ {
            checked = 1
            ok = fixtures && inventory
            exit
        }
        END { exit !(checked && ok) }
    ' "$dockerfile"; then
        report_error "$relative_path must COPY tests/fixtures and config/jma in AS build before Maven clean verify"
    fi
done

if [ "$failed" -ne 0 ]; then
    printf 'SPK-01 Java Docker build input check failed.\n' >&2
    exit 1
fi
printf 'SPK-01 Java Docker build input check passed.\n'
