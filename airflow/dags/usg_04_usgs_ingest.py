"""USG-04 daily USGS ingest orchestration DAG."""

from __future__ import annotations

from datetime import timedelta
import os

import pendulum
from airflow.exceptions import AirflowException
from airflow.sdk import dag, get_current_context, task, task_group

from usgs_ingest_runtime import (
    UsgsRunnerError,
    execute_phase,
    require_bronze_ready,
    resolve_run_context,
    write_run_summary,
)


DAG_ID = "usg_04_usgs_ingest"
PIPELINE_TIMEZONE = os.environ.get("PIPELINE_TIMEZONE", "Asia/Ho_Chi_Minh")
PIPELINE_SCHEDULE_CRON = os.environ.get("PIPELINE_SCHEDULE_CRON", "15 7 * * *")
TASK_RETRIES = int(os.environ.get("USGS_AIRFLOW_TASK_RETRIES", "2"))
TASK_RETRY_DELAY = int(os.environ.get("USGS_AIRFLOW_RETRY_DELAY_MINUTES", "5"))


@dag(
    dag_id=DAG_ID,
    description="Fetch, validate, write and verify one USGS Bronze window.",
    schedule=PIPELINE_SCHEDULE_CRON,
    start_date=pendulum.datetime(2025, 1, 1, tz=PIPELINE_TIMEZONE),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    default_args={
        "retries": TASK_RETRIES,
        "retry_delay": timedelta(minutes=TASK_RETRY_DELAY),
    },
    tags=["usgs", "bronze", "core"],
)
def usgs_ingest() -> None:
    """Resolve one window and publish only after Bronze verification succeeds."""

    @task(task_id="resolve_interval", retries=0)
    def resolve_interval() -> dict[str, object]:
        return resolve_run_context(get_current_context())

    @task(task_id="fetch")
    def fetch(run_context: dict[str, object]) -> dict[str, object]:
        return execute_phase("fetch", run_context)

    @task(task_id="validate")
    def validate(
        run_context: dict[str, object], fetch_result: dict[str, object]
    ) -> dict[str, object]:
        result = execute_phase("validate", run_context, fetch_result)
        if result.get("valid") is not True or result.get("bronze_status") == "Rejected":
            raise AirflowException("USGS payload was rejected before Bronze upload")
        return result

    @task(task_id="upload")
    def upload(
        run_context: dict[str, object], validation_result: dict[str, object]
    ) -> dict[str, object]:
        if validation_result.get("bronze_status") == "Rejected":
            raise AirflowException("Rejected payload cannot reach the Bronze upload task")
        return execute_phase("upload", run_context, validation_result)

    @task(task_id="verify")
    def verify(
        run_context: dict[str, object], upload_result: dict[str, object]
    ) -> dict[str, object]:
        result = execute_phase("verify", run_context, upload_result)
        try:
            require_bronze_ready(result)
        except UsgsRunnerError as exc:
            raise AirflowException(str(exc)) from exc
        return result

    @task(task_id="bronze_ready_gate", retries=0)
    def bronze_ready_gate(
        run_context: dict[str, object], verification_result: dict[str, object]
    ) -> dict[str, object]:
        try:
            gate = require_bronze_ready(verification_result)
        except UsgsRunnerError as exc:
            raise AirflowException(str(exc)) from exc
        return {"run_id": run_context["run_id"], **gate}

    @task(task_id="run_summary", retries=0)
    def run_summary(
        run_context: dict[str, object],
        fetch_result: dict[str, object],
        validation_result: dict[str, object],
        upload_result: dict[str, object],
        verification_result: dict[str, object],
        gate_result: dict[str, object],
    ) -> dict[str, object]:
        summary_uri = write_run_summary(
            run_context,
            {
                "fetch": fetch_result,
                "validate": validation_result,
                "upload": upload_result,
                "verify": verification_result,
                "bronze_ready_gate": gate_result,
            },
        )
        return {
            "run_id": run_context["run_id"],
            "status": "BronzeReady",
            "summary_uri": summary_uri,
            "manifest_uri": verification_result.get("manifest_uri"),
        }

    @task_group(group_id="usgs_ingest")
    def usgs_ingest_group() -> None:
        run_context = resolve_interval()
        fetch_result = fetch(run_context)
        validation_result = validate(run_context, fetch_result)
        upload_result = upload(run_context, validation_result)
        verification_result = verify(run_context, upload_result)
        gate_result = bronze_ready_gate(run_context, verification_result)
        run_summary(
            run_context,
            fetch_result,
            validation_result,
            upload_result,
            verification_result,
            gate_result,
        )

    usgs_ingest_group()


usgs_ingest()
