"""ORC-02 real source readiness. Silver/Gold adapters remain a separate ETL gate."""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.timetables.interval import CronDataIntervalTimetable

from source_run_guard import lease, owner
from source_schedule_runtime import ingest_usgs, profile, ready_summary, refresh_jma, resolve_daily
from run_observability import (dag_failure_summary, observe, source_context, source_details,
                               unresolved_context)

PROFILE = profile()


@dag(
    dag_id="orc_02_daily_sources",
    schedule=CronDataIntervalTimetable(PROFILE["cron"], timezone=PROFILE["timezone"])
    if PROFILE["mode"] == "multi-source" else None,
    start_date=pendulum.datetime(2023, 1, 1, tz=PROFILE["timezone"]),
    catchup=False, is_paused_upon_creation=True, max_active_runs=1, max_active_tasks=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    on_failure_callback=dag_failure_summary,
    tags=["orc-02", "daily", "readiness", "bronze"],
)
def daily_sources():
    @task(task_id="resolve_daily", retries=0)
    def resolve():
        ctx = get_current_context()
        return observe(unresolved_context(ctx, "orc_02_daily_sources"), "resolve",
                       lambda: resolve_daily(ctx),
                       lambda result: {"resolved_context_sha256": source_context(result)["context_sha256"]},
                       attempt=ctx["ti"].try_number)

    @task(task_id="acquire_source_lease", retries=0)
    def acquire(plan):
        def operation():
            return lease("acquire", owner(get_current_context()))
        return observe(source_context(plan), "readiness", operation, lambda result: {})

    @task(task_id="jma_readiness")
    def jma_ready(plan, acquired):
        ctx = get_current_context()
        def operation():
            lease("assert", owner(get_current_context()))
            return refresh_jma(plan, ctx["ti"].try_number)
        return observe(source_context(plan), "readiness", operation,
                       lambda result: source_details("JMA_BULLETIN", result["archives"]),
                       attempt=ctx["ti"].try_number)

    @task(task_id="usgs_bronze")
    def usgs_ready(plan, jma):
        def operation():
            lease("assert", owner(get_current_context()))
            return ingest_usgs(plan)
        return observe(source_context(plan), "bronze", operation,
                       lambda result: source_details("USGS", [result], plan["usgs"]),
                       attempt=get_current_context()["ti"].try_number)

    @task(task_id="sources_ready", retries=0)
    def summarize(plan, jma, usgs):
        return observe(source_context(plan), "verify", lambda: ready_summary(plan, jma, usgs),
                       lambda result: {})

    @task(task_id="release_source_lease", trigger_rule="all_done", retries=0)
    def release():
        return lease("release", owner(get_current_context()))

    @task(task_id="completion_gate", retries=0)
    def complete(plan, summary, released):
        def operation():
            if summary["status"] != "SourcesReady" or released["status"] != "RELEASED":
                raise RuntimeError("source completion gate failed")
            return {"status": "SourcesReady", "published": False}
        return observe(source_context(plan), "complete", operation,
                       lambda result: {"completion_status": "SourcesReady"})

    plan = resolve()
    acquired = acquire(plan)
    jma = jma_ready(plan, acquired)
    usgs = usgs_ready(plan, jma)
    summary = summarize(plan, jma, usgs)
    released = release()
    summary >> released
    complete(plan, summary, released)  # Strict sole leaf; cleanup cannot mask upstream failure.


daily_sources()
