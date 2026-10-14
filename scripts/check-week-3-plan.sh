#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
tasks_dir="$project_root/docs/task/tasks"
task_index="$tasks_dir/README.md"
task_plan="$project_root/docs/task/README.md"
week_plan="$project_root/docs/task/WEEK_3_PARALLEL_PLAN.md"

fail() {
    printf 'Week 3 parallel plan check failed: %s\n' "$1" >&2
    exit 1
}

task_field() {
    field=$1
    task_id=$2
    awk -F ': ' -v field="$field" '
        $1 == field {
            gsub(/"/, "", $2)
            print $2
            exit
        }
    ' "$tasks_dir/$task_id.md"
}

sum_effort() {
    total=0
    for task_id in "$@"; do
        effort=$(task_field effort_hours "$task_id")
        [ -n "$effort" ] || fail "$task_id has no effort_hours"
        total=$((total + effort))
    done
    printf '%s\n' "$total"
}

for required_file in \
    "$tasks_dir/USG-06.md" \
    "$tasks_dir/JMA-01.md" \
    "$tasks_dir/DAT-01.md" \
    "$task_index" \
    "$task_plan" \
    "$week_plan"
do
    [ -f "$required_file" ] || fail "missing ${required_file#"$project_root/"}"
done

set -- "$tasks_dir"/*-[0-9][0-9].md
[ "$#" -eq 76 ] || fail "expected 76 task files (73 baseline + ORC-06/07 + DAT-02), found $#"

total_effort=$(awk '$1 == "effort_hours:" { total += $2 } END { print total + 0 }' "$@")
[ "$total_effort" -eq 386 ] || fail "expected 386 total task hours, found $total_effort"
# Preserve historical estimates while checking explicitly added scope.
[ "$(sum_effort ORC-06 ORC-07)" -eq 16 ] || fail 'ORC-06/07 addendum must total 16 hours'
[ "$(sum_effort DAT-02)" -eq 6 ] || fail 'DAT-02 pilot addendum must total 6 hours'
[ "$((total_effort - 16 - 6))" -eq 364 ] || fail 'original backlog must retain 364 hours'

core_count=$(awk -F ': ' '$1 == "scope" && $2 == "\"Core\"" { count++ } END { print count + 0 }' "$@")
stretch_count=$(awk -F ': ' '$1 == "scope" && $2 == "\"Stretch\"" { count++ } END { print count + 0 }' "$@")
[ "$core_count" -eq 68 ] || fail "expected 68 Core tasks, found $core_count"
[ "$stretch_count" -eq 8 ] || fail "expected 8 Stretch tasks, found $stretch_count"

[ "$(sum_effort USG-06 JMA-01 DAT-01)" -eq 12 ] || \
    fail 'pre-week gate must total 12 hours'
[ "$(sum_effort JMA-02 SLV-01 SLV-05 JMA-03 SLV-03 SLV-02 SLV-04 SLV-08)" -eq 41 ] || \
    fail 'three implementation lanes must total 41 hours'

[ "$(task_field status USG-06)" = Done ] || fail 'USG-06 must be Done'
[ "$(task_field status JMA-01)" = Done ] || fail 'JMA-01 must be Done'
[ "$(task_field status DAT-01)" = Done ] || \
    fail 'DAT-01 must be Done before the week 3 integration lanes start'

for required_file in \
    "$project_root/docs/specs/SHARED_REAL_SAMPLE_DATA.md" \
    "$project_root/tests/fixtures/real-samples/catalog.json" \
    "$project_root/scripts/check-real-sample-catalog.sh"
do
    [ -f "$required_file" ] || fail "missing ${required_file#"$project_root/"}"
done

for task_id in SLV-01 SLV-02 SLV-03 SLV-04 SLV-05 SLV-08; do
    task_status=$(task_field status "$task_id")
    case "$task_status" in
        Ready|In\ Progress|Review|Done) ;;
        *) fail "$task_id must be Ready, In Progress, Review or Done" ;;
    esac
done

for expected in \
    'Tổng số task: **76**' \
    'Core: **68 task**' \
    'Stretch: **8 task**' \
    'Tổng effort task: **386 giờ**' \
    '[DAT-01](./DAT-01.md)' \
    '[USG-06](./USG-06.md)'
do
    grep -F -- "$expected" "$task_index" >/dev/null || \
        fail "task index is missing: $expected"
done

for expected in \
    '# Kế hoạch tuần 3 — ba luồng không chờ nhau' \
    'Tổng pre-week: **12 giờ**' \
    'Tổng implementation task: **41 giờ**' \
    'Bốn giờ còn lại trong capacity tuần'
do
    grep -F -- "$expected" "$week_plan" >/dev/null || \
        fail "week plan is missing: $expected"
done

grep -F -- 'còn **352 giờ task** trong 8 tuần' "$task_plan" >/dev/null || \
    fail 'eight-week capacity accounting is not documented'

if grep -E '(^|[^A-Z])(TBD|TODO|FIXME)([^A-Z]|$)' \
    "$tasks_dir/USG-06.md" "$tasks_dir/DAT-01.md" "$week_plan" >/dev/null; then
    fail 'new task documents contain unresolved placeholders'
fi

printf 'Week 3 historical lanes preserved: 12h pre-week + 41h lanes; current backlog 76 tasks/386h (364h + 16h + 6h).\n'
