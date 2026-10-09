"""Manual ETL skeleton. Mock success is not a real Gold publication."""

import os

import pendulum
from airflow.sdk import dag, get_current_context, task, task_group

from etl_pipeline_runtime import (execute_observed_phase, observed_publication_summary,
                                  resolve_observed_context)
from run_observability import dag_failure_summary


@dag(
    dag_id="orc_01_etl_pipeline",
    description="Guarded source readiness -> Bronze -> Silver -> Gold -> Trino -> publish.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz=os.environ.get("PIPELINE_TIMEZONE", "Asia/Ho_Chi_Minh")),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={"retries": 0, "trigger_rule": "all_success"},
    on_failure_callback=dag_failure_summary,
    tags=["etl", "contract", "orc-01"],
)
def etl_pipeline():
    @task(task_id="resolve_run_context")
    def resolve_context():
        ctx = get_current_context()
        return resolve_observed_context(ctx, attempt=ctx["ti"].try_number)

    @task(task_id="execute_adapter")
    def run_phase(phase, context, upstream=None):
        return execute_observed_phase(phase, context, upstream, attempt=get_current_context()["ti"].try_number)

    @task(task_id="contract_gate")
    def finish(context, published):
        return observed_publication_summary(context, published, attempt=get_current_context()["ti"].try_number)

    @task_group(group_id="source_readiness")
    def source_readiness(context):
        return run_phase("readiness", context)

    @task_group(group_id="bronze")
    def bronze(context, ready):
        return run_phase("bronze", context, ready)

    @task_group(group_id="silver")
    def silver(context, bronze_ready):
        return run_phase("silver", context, bronze_ready)

    @task_group(group_id="gold")
    def gold(context, silver_ready):
        return run_phase("gold", context, silver_ready)

    @task_group(group_id="verification")
    def verification(context, committed):
        return run_phase("verify", context, committed)

    @task_group(group_id="publication")
    def publication(context, verified):
        published = run_phase("publish", context, verified)
        return finish(context, published)

    context = resolve_context()
    ready = source_readiness(context)
    bronze_ready = bronze(context, ready)
    silver_ready = silver(context, bronze_ready)
    committed = gold(context, silver_ready)
    verified = verification(context, committed)
    publication(context, verified)


etl_pipeline()
