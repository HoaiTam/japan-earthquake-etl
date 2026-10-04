#!/usr/bin/env sh

set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
baseline="$project_root/docs/specs/MVP_SCOPE_KPI_AND_DOD.md"
task="$project_root/docs/task/tasks/PLN-01.md"
index="$project_root/docs/task/tasks/README.md"
roadmap="$project_root/docs/task/HDBSCAN_WORKSTREAM.md"

fail() {
    printf 'PLN-01 MVP baseline check failed: %s\n' "$1" >&2
    exit 1
}

for required_file in "$baseline" "$task" "$index" "$roadmap"; do
    [ -f "$required_file" ] || fail "missing ${required_file#"$project_root/"}"
done

required_baseline_values='
| Trạng thái tài liệu | Baseline HDBSCAN hiện hành |
8 tuần, 3 thành viên, 357 giờ
Window, DBSCAN, HDBSCAN global và HDBSCAN adaptive
Power BI là Stretch và không chặn MVP
2018-10-01T00:00:00Z
2024-01-01T00:00:00Z
(dataset_id, mainshock_event_id, candidate_event_id)
[x_scaled, y_scaled, z_scaled, time_scaled]
ml.dataset_manifest
ml.sequence_membership
_SUCCESS.json
Modified Omori
không phải dự đoán hay chứng minh quan hệ nhân quả
'

printf '%s\n' "$required_baseline_values" | while IFS= read -r expected; do
    [ -n "$expected" ] || continue
    grep -F -- "$expected" "$baseline" >/dev/null || \
        fail "missing baseline decision: $expected"
done

if grep -F -- 'Needs Update' "$baseline" >/dev/null; then
    fail "baseline still contains transitional Needs Update state"
fi

grep -F -- 'status: "Done"' "$task" >/dev/null || \
    fail "PLN-01 task metadata is not Done"
grep -F -- '| [PLN-01](./PLN-01.md) - Chốt scope, KPI và Definition of Done | 1 | Planning | Core | P0 | 4h | Done |' "$index" >/dev/null || \
    fail "task index does not mark PLN-01 Done"
grep -F -- '| `PLN-01` | `Done` |' "$roadmap" >/dev/null || \
    fail "HDBSCAN roadmap does not mark PLN-01 Done"
grep -F -- '| `CON-03` | `Needs Update` |' "$roadmap" >/dev/null || \
    fail "CON-03 update requirement was lost"

printf 'PLN-01 HDBSCAN MVP baseline check passed.\n'
