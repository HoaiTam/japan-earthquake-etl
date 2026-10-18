"""Build ML dataset orchestration DAG (MLI-02).

Orchestrates Gold snapshot to export bundle:
wait_gold_published → resolve_gold_snapshot → audit_gold_input → resolve_mc
→ select_mainshocks → build_candidate_windows → build_spacetime_features
→ validate_feature_snapshot → export_parquet → mark_dataset_exported

Guarded by the shared staging lease; Colab handoff is strictly manual
(no auto-trigger of Colab as an external job service).
"""

from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

from ml_dataset_runtime import (
    DAG_ID,
    execute_step,
    mark_dataset_exported_summary,
    resolve_run_context,
)
try:
    from run_observability import dag_failure_summary
except (ModuleNotFoundError, ImportError):  # pragma: no cover - Windows offline tests
    def dag_failure_summary(context: Any) -> None:
        pass


@dag(
    dag_id=DAG_ID,
    description="ML dataset build: wait_gold_published -> snapshot -> audit -> mc -> mainshock -> window -> features -> validate -> export -> mark_exported.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=os.environ.get("PIPELINE_TIMEZONE", "Asia/Ho_Chi_Minh")),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 0, "trigger_rule": "all_success"},
    on_failure_callback=dag_failure_summary,
    tags=["ml", "dataset", "mli-02"],
)
def dataset_build_pipeline():
    @task(task_id="resolve_run_context")
    def resolve_context():
        ctx = get_current_context()
        conf = (ctx.get("dag_run").conf or {}) if ctx.get("dag_run") else {}
        raw = {"conf": conf, "run_id": ctx["run_id"], "dag_id": DAG_ID}
        return resolve_run_context(raw, attempt=ctx["ti"].try_number)

    @task(task_id="execute_step")
    def run_step(step_name, context, upstream=None):
        ctx = get_current_context()
        return execute_step(step_name, context, upstream, attempt=ctx["ti"].try_number)

    @task(task_id="contract_gate")
    def finish(context, final_result):
        return mark_dataset_exported_summary(context, final_result)

    @task_group(group_id="wait_gold_published")
    def wait_gold_published(context):
        return run_step("wait_gold_published", context)

    @task_group(group_id="resolve_gold_snapshot")
    def resolve_gold_snapshot(context, upstream):
        return run_step("resolve_gold_snapshot", context, upstream)

    @task_group(group_id="audit_gold_input")
    def audit_gold_input(context, upstream):
        return run_step("audit_gold_input", context, upstream)

    @task_group(group_id="resolve_mc")
    def resolve_mc(context, upstream):
        return run_step("resolve_mc", context, upstream)

    @task_group(group_id="select_mainshocks")
    def select_mainshocks(context, upstream):
        return run_step("select_mainshocks", context, upstream)

    @task_group(group_id="build_candidate_windows")
    def build_candidate_windows(context, upstream):
        return run_step("build_candidate_windows", context, upstream)

    @task_group(group_id="build_spacetime_features")
    def build_spacetime_features(context, upstream):
        return run_step("build_spacetime_features", context, upstream)

    @task_group(group_id="validate_feature_snapshot")
    def validate_feature_snapshot(context, upstream):
        return run_step("validate_feature_snapshot", context, upstream)

    @task_group(group_id="export_parquet")
    def export_parquet(context, upstream):
        return run_step("export_parquet", context, upstream)

    @task_group(group_id="mark_dataset_exported")
    def mark_dataset_exported(context, upstream):
        exported = run_step("mark_dataset_exported", context, upstream)
        return finish(context, exported)

    context = resolve_context()
    p1 = wait_gold_published(context)
    p2 = resolve_gold_snapshot(context, p1)
    p3 = audit_gold_input(context, p2)
    p4 = resolve_mc(context, p3)
    p5 = select_mainshocks(context, p4)
    p6 = build_candidate_windows(context, p5)
    p7 = build_spacetime_features(context, p6)
    p8 = validate_feature_snapshot(context, p7)
    p9 = export_parquet(context, p8)
    mark_dataset_exported(context, p9)


dataset_build_pipeline()
