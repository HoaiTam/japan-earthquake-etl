"""Manual, explicit-scope JMA year/segment backfill. No live calls at import time."""

from datetime import timedelta
import os

import pendulum
from airflow.exceptions import AirflowException
from airflow.sdk import dag, get_current_context, task, task_group

from jma_backfill_runtime import JmaRunnerError, execute_archive, require_complete, resolve_plan, write_run_summary
from source_run_guard import lease, owner

MAX_CONCURRENCY = int(os.environ.get("JMA_BACKFILL_MAX_CONCURRENCY", "2"))
if not 1 <= MAX_CONCURRENCY <= 4:
    raise ValueError("JMA_BACKFILL_MAX_CONCURRENCY must be within 1..4")


@dag(
    dag_id="jma_04_year_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=os.environ.get("PIPELINE_TIMEZONE", "Asia/Ho_Chi_Minh")),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=MAX_CONCURRENCY + 3,
    tags=["jma", "bronze", "backfill"],
)
def jma_backfill():
    @task(task_id="resolve_plan", retries=0)
    def plan_scope():
        return resolve_plan(get_current_context())

    @task(task_id="select_archives", retries=0)
    def select_archives(plan, acquired):
        return [] if plan["preview"] else plan["archives"]

    @task(task_id="acquire_source_lease", retries=0)
    def acquire_source_lease(plan):
        if plan["preview"]:
            return {"status": "PREVIEW"}
        return lease("acquire", owner(get_current_context()))

    @task(task_id="release_source_lease", trigger_rule="all_done", retries=0)
    def release_source_lease():
        return lease("release", owner(get_current_context()))

    @task(task_id="completion_gate", retries=0)
    def completion_gate(ready, acquired, released):
        if acquired["status"] != "PREVIEW" and released["status"] != "RELEASED":
            raise AirflowException("source lease release failed")
        return ready

    @task(task_id="ingest_archive", retries=2, retry_delay=timedelta(minutes=2),
          max_active_tis_per_dag=MAX_CONCURRENCY)
    def ingest_archive(plan, archive):
        lease("assert", owner(get_current_context()))
        attempt = get_current_context()["ti"].try_number
        result = execute_archive(plan, archive, attempt)
        if result.get("status") != "BronzeReady" or result.get("verified") is not True:
            raise AirflowException("JMA archive failed; scoped metadata saved in staging")
        return result

    @task(task_id="run_summary", trigger_rule="all_done", retries=0)
    def summarize(plan):
        return write_run_summary(plan)

    @task(task_id="bronze_ready_gate", retries=0)
    def gate(summary):
        try:
            return require_complete(summary)
        except JmaRunnerError as exception:
            raise AirflowException(str(exception)) from exception

    @task_group(group_id="jma_year_backfill")
    def backfill_group():
        plan = plan_scope()
        acquired = acquire_source_lease(plan)
        archives = select_archives(plan, acquired)
        ingested = ingest_archive.partial(plan=plan).expand(archive=archives)
        summary = summarize(plan)
        ingested >> summary
        ready = gate(summary)
        released = release_source_lease()
        ready >> released
        completion_gate(ready, acquired, released)

    backfill_group()


jma_backfill()
