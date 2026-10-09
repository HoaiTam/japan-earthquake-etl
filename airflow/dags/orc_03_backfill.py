"""Manual, preview-first scoped ingestion/reprocessing with cross-DAG exclusion."""
import pendulum
from airflow.sdk import dag, get_current_context, task

from backfill_runtime import execute, preview_summary
import backfill_observability as telemetry
from run_observability import dag_failure_summary


@dag(dag_id="orc_03_backfill", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, is_paused_upon_creation=True, max_active_runs=1, max_active_tasks=1,
     default_args={"retries": 0}, on_failure_callback=dag_failure_summary, tags=["orc-03", "backfill", "reprocess"])
def backfill():
    @task(task_id="resolve_plan")
    def plan():
        ctx = get_current_context()
        return telemetry.resolve_observed(ctx, attempt=ctx["ti"].try_number)

    @task(task_id="acquire_source_lease")
    def acquire(resolved):
        if resolved["preview"]:
            return {"status": "PREVIEW"}
        ctx = get_current_context()
        return telemetry.source_lease(resolved, ctx["run_id"], "acquire", attempt=ctx["ti"].try_number)

    @task(task_id="execute_scope")
    def run(resolved, acquired):
        if resolved["preview"]:
            return preview_summary(resolved)
        ctx = get_current_context()
        return execute(resolved, attempt=ctx["ti"].try_number,
                       telemetry_context=telemetry.context(resolved, ctx["run_id"]))

    @task(task_id="release_source_lease", trigger_rule="all_done")
    def release(resolved):
        if resolved["preview"]:
            return {"status": "PREVIEW"}
        ctx = get_current_context()
        return telemetry.source_lease(resolved, ctx["run_id"], "release", attempt=ctx["ti"].try_number)

    @task(task_id="completion_gate")
    def complete(resolved, summary, released):
        if resolved["preview"]:
            if summary["status"] != "PREVIEW" or summary["published"] is not False:
                raise RuntimeError("PREVIEW_GATE_FAILED")
        else:
            ctx = get_current_context()
            return telemetry.complete(resolved, ctx["run_id"], summary, released, attempt=ctx["ti"].try_number)
        return summary

    resolved = plan()
    acquired = acquire(resolved)
    summary = run(resolved, acquired)
    released = release(resolved)
    summary >> released
    complete(resolved, summary, released)  # all_success sole leaf: cleanup cannot hide failed work.


backfill()
