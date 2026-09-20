"""Minimal scheduler smoke DAG for the AFL-01 local Airflow runtime."""

from __future__ import annotations

import logging
import os

import pendulum
from airflow.sdk import dag, get_current_context, task

LOGGER = logging.getLogger(__name__)
PIPELINE_TIMEZONE = os.environ.get("PIPELINE_TIMEZONE", "Asia/Ho_Chi_Minh")


@dag(
    dag_id="afl_01_smoke",
    description="Verify that the local Airflow scheduler can execute a task.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz=PIPELINE_TIMEZONE),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    tags=["foundation", "smoke"],
)
def airflow_local_smoke() -> None:
    """Define one deterministic task without network or external data access."""

    @task(task_id="verify_scheduler_execution")
    def verify_scheduler_execution() -> dict[str, str]:
        context = get_current_context()
        run_id = str(context["run_id"])
        LOGGER.info(
            "event=airflow_smoke_success dag_id=afl_01_smoke run_id=%s",
            run_id,
        )
        return {"status": "ok", "run_id": run_id}

    verify_scheduler_execution()


airflow_local_smoke()
