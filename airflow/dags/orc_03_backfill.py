"""Manual, preview-first scoped ingestion/reprocessing with cross-DAG exclusion."""
import pendulum
from airflow.sdk import dag, get_current_context, task

from backfill_runtime import execute, preview_summary, resolve
from source_run_guard import lease, owner


@dag(dag_id="orc_03_backfill", schedule=None, start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
     catchup=False, is_paused_upon_creation=True, max_active_runs=1, max_active_tasks=1,
     default_args={"retries": 0}, tags=["orc-03", "backfill", "reprocess"])
def backfill():
    @task(task_id="resolve_plan")
    def plan():
        return resolve(get_current_context())

    @task(task_id="acquire_source_lease")
    def acquire(resolved):
        if resolved["preview"]:
            return {"status": "PREVIEW"}
        return lease("acquire", owner(get_current_context()))

    @task(task_id="execute_scope")
    def run(resolved, acquired):
        if resolved["preview"]:
            return preview_summary(resolved)
        lease("assert", owner(get_current_context()))
        return execute(resolved, attempt=get_current_context()["ti"].try_number)

    @task(task_id="release_source_lease", trigger_rule="all_done")
    def release(resolved):
        if resolved["preview"]:
            return {"status": "PREVIEW"}
        return lease("release", owner(get_current_context()))

    @task(task_id="completion_gate")
    def complete(resolved, summary, released):
        if resolved["preview"]:
            if summary["status"] != "PREVIEW" or summary["published"] is not False:
                raise RuntimeError("PREVIEW_GATE_FAILED")
        elif (released["status"] != "RELEASED" or summary["verified"] is not True
              or summary["scope_sha256"] != resolved["scope_sha256"]
              or summary["status"] != ("Published" if resolved["action"] == "reprocess" else "BronzeVerified")):
            raise RuntimeError("BACKFILL_COMPLETION_FAILED")
        return summary

    resolved = plan()
    acquired = acquire(resolved)
    summary = run(resolved, acquired)
    released = release(resolved)
    summary >> released
    complete(resolved, summary, released)  # all_success sole leaf: cleanup cannot hide failed work.


backfill()
