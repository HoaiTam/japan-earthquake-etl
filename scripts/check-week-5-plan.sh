#!/usr/bin/env sh
# Metadata only: no Docker, .env, network or business-data processing.
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
tasks_dir="${WEEK5_TASKS_DIR:-$project_root/docs/task/tasks}"
plan="$project_root/docs/task/WEEK_5_PARALLEL_PLAN.md"
fail() { printf 'Week 5 plan check failed: %s\n' "$1" >&2; exit 1; }
field() {
    awk -F ': ' -v key="$1" '$1 == key { gsub(/"/, "", $2); print $2; exit }' "$tasks_dir/$2.md"
}
lane() {
    owner=$1; expected=$2; shift 2; total=0
    for id in "$@"; do
        [ -f "$tasks_dir/$id.md" ] || fail "missing $id"
        [ "$(field week "$id")" = 5 ] || fail "$id week must be 5"
        [ "$(field assignee "$id")" = "$owner" ] || fail "$id owner must be $owner"
        [ "$(field scope "$id")" = Core ] || fail "$id must stay Core"
        grep -F -- "[$id](./tasks/$id.md)" "$plan" >/dev/null || fail "plan lacks $id link"
        hours=$(field effort_hours "$id")
        case "$hours" in ''|*[!0-9]*) fail "$id invalid hours";; esac
        total=$((total + hours))
    done
    [ "$total" -eq "$expected" ] || fail "$owner expected ${expected}h, found ${total}h"
}
lane HoaiTam 24 DAT-02 ORC-07 MLI-02 QA-01
lane ThanhTris 25 ORC-06 MLI-03 QA-02 QA-03
lane rosy179 23 MLD-04 MLD-05 EXP-01 EXP-02
count=0
for file in "$tasks_dir"/*-[0-9][0-9].md; do
    id=${file##*/}; id=${id%.md}
    if [ "$(field week "$id")" = 5 ] && [ "$(field scope "$id")" = Core ]; then
        count=$((count + 1))
    fi
done
[ "$count" -eq 12 ] || fail "expected 12 Core week-5 tasks, found $count"
for id in QA-01 QA-02 QA-03 QA-04; do
    grep '^dependencies: ' "$tasks_dir/$id.md" | grep -F '"ORC-07"' >/dev/null ||
        fail "$id lacks real adapter hard gate"
done
for id in ORC-06 ORC-07; do
    [ "$(field effort_hours "$id")" = 8 ] || fail "$id must be 8h"
done
requires() {
    grep '^dependencies: ' "$tasks_dir/$1.md" | grep -F "\"$2\"" >/dev/null ||
        fail "$1 lacks $2 dependency"
}
[ "$(field effort_hours DAT-02)" = 6 ] || fail 'DAT-02 must be 6h'
[ "$(field week QA-04)" = 6 ] || fail 'QA-04 must be deferred to week 6'
requires DAT-02 MLD-03
requires DAT-02 MLD-04
requires EXP-01 DAT-02
requires EXP-02 EXP-01
requires EXP-02 DAT-02
requires EXP-02 MLD-05
for id in DAT-02 MLD-04 MLD-05; do
    if grep '^dependencies: ' "$tasks_dir/$id.md" | grep -E '"EXP-0[12]"' >/dev/null; then
        fail "$id creates reverse experiment dependency"
    fi
done
if grep '^dependencies: ' "$tasks_dir/MLD-04.md" | grep -F '"DAT-02"' >/dev/null; then
    fail 'MLD-04 creates pilot dependency cycle'
fi
if grep '^dependencies: ' "$tasks_dir/EXP-01.md" | grep -F '"MLD-05"' >/dev/null; then
    fail 'EXP-01 local harness must not wait for official MLD-05 export'
fi
[ "$(field week BI-02)" = 8 ] || fail 'BI-02 must be deferred to week 8'
for id in MLD-02 MLD-03; do
    [ "$(field assignee "$id")" = rosy179 ] || fail "$id owner exception lost"
    [ "$(field reviewer "$id")" = HoaiTam ] || fail "$id reviewer exception lost"
done
printf 'Week 5 plan check passed: 12 Core tasks, 72h (24/25/23); DAT-02 Jupyter first, Colab gated, QA-04 week 6.\n'
