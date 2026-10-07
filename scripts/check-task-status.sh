#!/usr/bin/env sh

# Keep task frontmatter, tracking text and the shared index in sync. No rg/Python required.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
tasks_dir=${TASK_STATUS_DIR:-"$project_root/docs/task/tasks"}
index="$tasks_dir/README.md"

fail() {
    printf 'Task status check failed: %s\n' "$1" >&2
    exit 1
}

[ -f "$index" ] || fail 'missing task index'
count=0
for task_file in "$tasks_dir"/*-[0-9][0-9].md; do
    [ -f "$task_file" ] || fail 'no task files found'
    task_id=$(sed -n 's/^task_id: "\([^"]*\)"$/\1/p' "$task_file")
    status=$(sed -n 's/^status: "\([^"]*\)"$/\1/p' "$task_file")
    tracking=$(sed -n 's/^- \*\*Trạng thái:\*\* //p' "$task_file")
    [ "${task_file##*/}" = "$task_id.md" ] || fail "invalid task_id in $task_file"
    case "$status" in
        Backlog|Ready|In\ Progress|Review|Blocked|Needs\ Update|Done) ;;
        *) fail "$task_id has an invalid status: $status" ;;
    esac
    [ "$tracking" = "$status" ] || fail "$task_id metadata=$status but tracking=$tracking"
    index_status=$(awk -F '|' -v id="$task_id" '
        index($2, "[" id "](./" id ".md)") {
            value = $(NF - 1)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
            print value
        }
    ' "$index")
    [ "$index_status" = "$status" ] || fail "$task_id metadata=$status but index=$index_status"
    count=$((count + 1))
done
index_count=$(awk -F '|' '$2 ~ /\[[A-Z]+-[0-9][0-9]\]\(/ { count++ } END { print count + 0 }' "$index")
[ "$index_count" -eq "$count" ] || fail "index has $index_count rows but $count task files"
printf 'Task status check passed: %s task files, tracking sections and index rows agree.\n' "$count"
