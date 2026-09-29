#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
contract="$project_root/docs/specs/SOURCE_COVERAGE.md"
task="$project_root/docs/task/tasks/CON-01.md"
index="$project_root/docs/task/tasks/README.md"

fail() {
    printf 'CON-01 source coverage check failed: %s\n' "$1" >&2
    exit 1
}

[ -f "$contract" ] || fail "missing docs/specs/SOURCE_COVERAGE.md"
[ -f "$task" ] || fail "missing docs/task/tasks/CON-01.md"

required_contract_values='
contract_id: "CON-01"
contract_version: "1.0"
study_area_id: "japan-regional-v1"
roi_min_latitude: 20.0
roi_max_latitude: 50.0
roi_min_longitude: 120.0
roi_max_longitude: 155.0
jma_baseline_start_year: 1984
jma_baseline_end_year: 2023
usgs_seed_start_utc: "2023-01-01T00:00:00Z"
usgs_revision_window_days: 3
https://earthquake.usgs.gov/fdsnws/event/1/
https://www.data.jma.go.jp/eqev/data/bulletin/hypo_e.html
## 7. Overlap và source priority
## 10. Acceptance scenarios
'

printf '%s\n' "$required_contract_values" | while IFS= read -r expected; do
    [ -n "$expected" ] || continue
    grep -F -- "$expected" "$contract" >/dev/null || fail "missing contract value: $expected"
done

if grep -Eq '(^|[^A-Z])(TBD|TODO|FIXME)([^A-Z]|$)' "$contract"; then
    fail "contract contains unresolved placeholder"
fi

grep -F -- '[Source coverage contract](../../specs/SOURCE_COVERAGE.md)' "$task" >/dev/null || \
    fail "CON-01 task does not link the contract"
grep -F -- '| [CON-01](./CON-01.md)' "$index" >/dev/null || \
    fail "task index does not contain CON-01"

year_count=$((2023 - 1984 + 1))
[ "$year_count" -eq 40 ] || fail "JMA baseline is not 40 complete years"

printf 'CON-01 source coverage contract check passed.\n'
