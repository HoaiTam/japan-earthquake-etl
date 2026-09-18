#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
example_file="$project_root/.env.example"
env_file=${ENV_FILE:-"$project_root/.env"}
require_local=0

case "${1:-}" in
    "") ;;
    --require-local) require_local=1 ;;
    *)
        printf 'Usage: %s [--require-local]\n' "$0" >&2
        exit 2
        ;;
esac

required_keys="
CONFIG_VERSION
PIPELINE_TIMEZONE
PIPELINE_SCHEDULE_CRON
PIPELINE_OVERLAP_DAYS
USGS_API_BASE_URL
STRONG_MAGNITUDE_THRESHOLD
DATA_BUCKET
BRONZE_PREFIX
SILVER_PREFIX
WAREHOUSE_PATH
MINIO_ENDPOINT
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
MINIO_CONSOLE_HOST_PORT
AIRFLOW_UID
AIRFLOW_ADMIN_USERNAME
AIRFLOW_ADMIN_PASSWORD
AIRFLOW_FERNET_KEY
AIRFLOW_WEBSERVER_SECRET_KEY
AIRFLOW_WEB_HOST_PORT
AIRFLOW_DB_USER
AIRFLOW_DB_PASSWORD
AIRFLOW_DB_NAME
SPARK_MASTER_URL
SPARK_DRIVER_MEMORY
SPARK_EXECUTOR_MEMORY
ICEBERG_CATALOG_URI
ICEBERG_CATALOG_NAME
TRINO_HOST
TRINO_INTERNAL_PORT
TRINO_HOST_PORT
TRINO_CATALOG
TRINO_SCHEMA
"

secret_keys="
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
AIRFLOW_ADMIN_PASSWORD
AIRFLOW_FERNET_KEY
AIRFLOW_WEBSERVER_SECRET_KEY
AIRFLOW_DB_PASSWORD
"

failed=0

report_error() {
    printf 'ERROR: %s\n' "$1" >&2
    failed=1
}

read_value() {
    file=$1
    target_key=$2

    awk -v target_key="$target_key" '
        /^[[:space:]]*($|#)/ { next }
        {
            separator = index($0, "=")
            key = substr($0, 1, separator - 1)
            if (key == target_key) {
                value = substr($0, separator + 1)
                gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
                first = substr(value, 1, 1)
                last = substr(value, length(value), 1)
                if ((first == "\"" && last == "\"") ||
                    (first == "\047" && last == "\047")) {
                    value = substr(value, 2, length(value) - 2)
                }
                print value
                exit
            }
        }
    ' "$file"
}

validate_structure() {
    file=$1
    label=$2

    if [ ! -f "$file" ]; then
        report_error "$label does not exist: $file"
        return
    fi

    invalid_lines=$(awk '
        /^[[:space:]]*($|#)/ { next }
        !/^[A-Za-z_][A-Za-z0-9_]*=/ { print NR }
    ' "$file")
    if [ -n "$invalid_lines" ]; then
        report_error "$label has invalid assignment syntax at line(s): $invalid_lines"
    fi

    duplicate_keys=$(awk -F= '
        /^[[:space:]]*($|#)/ { next }
        /^[A-Za-z_][A-Za-z0-9_]*=/ {
            seen[$1]++
            if (seen[$1] == 2) print $1
        }
    ' "$file")
    if [ -n "$duplicate_keys" ]; then
        report_error "$label has duplicate key(s): $duplicate_keys"
    fi

    for key in $required_keys; do
        if ! grep -Eq "^${key}=" "$file"; then
            report_error "$label is missing required key: $key"
            continue
        fi

        value=$(read_value "$file" "$key")
        if [ -z "$value" ]; then
            report_error "$label has an empty required key: $key"
        fi
    done
}

validate_example_secrets() {
    for key in $secret_keys; do
        value=$(read_value "$example_file" "$key")
        case "$value" in
            change-me-*) ;;
            *) report_error ".env.example must use a change-me-* placeholder for: $key" ;;
        esac
    done
}

validate_local_secrets() {
    for key in $secret_keys; do
        value=$(read_value "$env_file" "$key")
        case "$value" in
            ""|change-me-*|replace-*|example*)
                report_error ".env still has an empty or placeholder secret: $key"
                ;;
        esac
    done
}

validate_local_secret_strength() {
    strong_secret_keys="
MINIO_ROOT_PASSWORD
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
AIRFLOW_ADMIN_PASSWORD
AIRFLOW_WEBSERVER_SECRET_KEY
AIRFLOW_DB_PASSWORD
"

    for key in $strong_secret_keys; do
        value=$(read_value "$env_file" "$key")
        if [ "${#value}" -lt 16 ]; then
            report_error ".env secret must contain at least 16 characters: $key"
        fi
    done

    fernet_key=$(read_value "$env_file" AIRFLOW_FERNET_KEY)
    if [ "${#fernet_key}" -ne 44 ]; then
        report_error "AIRFLOW_FERNET_KEY must be a 44-character URL-safe Base64 key"
        return
    fi

    fernet_body=${fernet_key%?}
    fernet_padding=${fernet_key#"$fernet_body"}

    case "$fernet_body" in
        *[!A-Za-z0-9_-]*)
            report_error "AIRFLOW_FERNET_KEY contains invalid URL-safe Base64 characters"
            ;;
    esac

    if [ "$fernet_padding" != "=" ]; then
        report_error "AIRFLOW_FERNET_KEY must end with one Base64 padding character (=)"
    fi
}

validate_integer() {
    file=$1
    key=$2
    minimum=$3
    maximum=$4
    value=$(read_value "$file" "$key")

    case "$value" in
        ""|*[!0-9]*)
            report_error "$key must be an integer"
            return
            ;;
    esac

    if [ "$value" -lt "$minimum" ] || [ "$value" -gt "$maximum" ]; then
        report_error "$key must be between $minimum and $maximum"
    fi
}

validate_semantics() {
    file=$1

    validate_integer "$file" CONFIG_VERSION 1 999999
    validate_integer "$file" PIPELINE_OVERLAP_DAYS 0 999999
    validate_integer "$file" MINIO_CONSOLE_HOST_PORT 1 65535
    validate_integer "$file" AIRFLOW_UID 1 2147483647
    validate_integer "$file" AIRFLOW_WEB_HOST_PORT 1 65535
    validate_integer "$file" TRINO_INTERNAL_PORT 1 65535
    validate_integer "$file" TRINO_HOST_PORT 1 65535

    timezone=$(read_value "$file" PIPELINE_TIMEZONE)
    if [ "$timezone" != "Asia/Ho_Chi_Minh" ]; then
        report_error "PIPELINE_TIMEZONE must match the project baseline: Asia/Ho_Chi_Minh"
    fi

    minio_endpoint=$(read_value "$file" MINIO_ENDPOINT)
    case "$minio_endpoint" in
        http://*|https://*) ;;
        *) report_error "MINIO_ENDPOINT must be an http:// or https:// URI" ;;
    esac

    usgs_endpoint=$(read_value "$file" USGS_API_BASE_URL)
    case "$usgs_endpoint" in
        https://*) ;;
        *) report_error "USGS_API_BASE_URL must be an https:// URI" ;;
    esac

    spark_master=$(read_value "$file" SPARK_MASTER_URL)
    case "$spark_master" in
        spark://*) ;;
        *) report_error "SPARK_MASTER_URL must be a spark:// URI" ;;
    esac

    catalog_uri=$(read_value "$file" ICEBERG_CATALOG_URI)
    case "$catalog_uri" in
        http://*|https://*) ;;
        *) report_error "ICEBERG_CATALOG_URI must be an http:// or https:// URI" ;;
    esac

    bucket=$(read_value "$file" DATA_BUCKET)
    warehouse=$(read_value "$file" WAREHOUSE_PATH)
    case "$warehouse" in
        "s3://$bucket/"*) ;;
        *) report_error "WAREHOUSE_PATH must be inside DATA_BUCKET" ;;
    esac

    minio_host_port=$(read_value "$file" MINIO_CONSOLE_HOST_PORT)
    airflow_host_port=$(read_value "$file" AIRFLOW_WEB_HOST_PORT)
    trino_host_port=$(read_value "$file" TRINO_HOST_PORT)
    if [ "$minio_host_port" = "$airflow_host_port" ] ||
       [ "$minio_host_port" = "$trino_host_port" ] ||
       [ "$airflow_host_port" = "$trino_host_port" ]; then
        report_error "MINIO, Airflow and Trino host ports must be unique"
    fi
}

validate_git_hygiene() {
    if ! git -C "$project_root" check-ignore -q .env; then
        report_error ".env must be ignored by Git"
    fi

    if git -C "$project_root" check-ignore -q .env.example; then
        report_error ".env.example must not be ignored by Git"
    fi

    if git -C "$project_root" ls-files --error-unmatch .env >/dev/null 2>&1; then
        report_error ".env is tracked by Git"
    fi
}

scan_candidate_files() {
    private_key_pattern='-----BEGIN (RSA |EC |OPENSSH |DSA )?PRIVATE[[:space:]]KEY-----'
    aws_access_key_pattern='AKIA[0-9A-Z]{16}'
    github_token_pattern='gh[pousr]_[A-Za-z0-9]{36,}'
    google_api_key_pattern='AIza[0-9A-Za-z_-]{35}'

    old_ifs=$IFS
    IFS='
'
    for relative_path in $(git -C "$project_root" ls-files -co --exclude-standard); do
        candidate="$project_root/$relative_path"
        [ -f "$candidate" ] || continue

        for pattern in \
            "$private_key_pattern" \
            "$aws_access_key_pattern" \
            "$github_token_pattern" \
            "$google_api_key_pattern"
        do
            if LC_ALL=C grep -I -E -q -- "$pattern" "$candidate"; then
                report_error "potential credential pattern found in: $relative_path"
                break
            fi
        done

        for key in $secret_keys; do
            if LC_ALL=C grep -I -E -q "^[[:space:]]*${key}=" "$candidate"; then
                case "$relative_path" in
                    .env.example|.env.*.example) ;;
                    *) report_error "secret assignment found outside an example env file: $relative_path ($key)" ;;
                esac
            fi
        done
    done
    IFS=$old_ifs
}

validate_structure "$example_file" ".env.example"
validate_example_secrets
validate_semantics "$example_file"
validate_git_hygiene

if [ -f "$env_file" ]; then
    validate_structure "$env_file" ".env"
    validate_local_secrets
    validate_local_secret_strength
    validate_semantics "$env_file"
else
    if [ "$require_local" -eq 1 ]; then
        report_error "local .env not found: $env_file"
    else
        printf 'INFO: local .env not found; repository/example checks only. Use --require-local before runtime.\n'
    fi
fi

scan_candidate_files

if [ "$failed" -ne 0 ]; then
    printf 'CFG-01 configuration and secret hygiene check failed.\n' >&2
    exit 1
fi

printf 'CFG-01 configuration and secret hygiene check passed.\n'
