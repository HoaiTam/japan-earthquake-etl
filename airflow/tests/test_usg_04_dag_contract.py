"""Static contract tests for the USG-04 Airflow DAG."""

from __future__ import annotations

import ast
from pathlib import Path
import unittest


class UsgsIngestDagContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_path = Path(__file__).parents[1] / "dags" / "usg_04_usgs_ingest.py"
        cls.runtime_path = Path(__file__).parents[1] / "dags" / "usgs_ingest_runtime.py"
        cls.source = cls.dag_path.read_text(encoding="utf-8")
        cls.runtime_source = cls.runtime_path.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source, filename=str(cls.dag_path))
        cls.runtime_tree = ast.parse(cls.runtime_source, filename=str(cls.runtime_path))

    def test_dag_and_runtime_are_valid_python(self) -> None:
        self.assertIsInstance(self.tree, ast.Module)
        self.assertIsInstance(self.runtime_tree, ast.Module)

    def test_dag_is_visible_and_scheduled_with_project_timezone(self) -> None:
        self.assertIn('dag_id=DAG_ID', self.source)
        self.assertIn('DAG_ID = "usg_04_usgs_ingest"', self.source)
        self.assertIn("schedule=PIPELINE_SCHEDULE_CRON", self.source)
        self.assertIn("PIPELINE_TIMEZONE", self.source)
        self.assertIn("catchup=False", self.source)
        self.assertIn("is_paused_upon_creation=True", self.source)
        self.assertIn("max_active_runs=1", self.source)

    def test_task_group_has_all_bronze_gates_and_run_summary(self) -> None:
        for task_id in (
            "resolve_interval",
            "fetch",
            "validate",
            "upload",
            "verify",
            "bronze_ready_gate",
            "run_summary",
        ):
            self.assertIn(f'task_id="{task_id}"', self.source)
        self.assertIn('@task_group(group_id="usgs_ingest")', self.source)
        self.assertIn("verification_result = verify", self.source)
        self.assertIn("gate_result = bronze_ready_gate", self.source)

    def test_retry_and_window_context_are_explicit(self) -> None:
        self.assertIn('"retries": TASK_RETRIES', self.source)
        self.assertIn('"retry_delay": timedelta(minutes=TASK_RETRY_DELAY)', self.source)
        self.assertIn('return resolve_run_context(get_current_context())', self.source)
        self.assertIn('"logical_run_key"', self.runtime_source)
        self.assertIn('"window_start_utc"', self.runtime_source)
        self.assertIn('"window_end_utc"', self.runtime_source)

    def test_runner_does_not_put_payload_in_xcom_or_logs(self) -> None:
        self.assertIn("USGS_INGEST_RUNNER_COMMAND", self.runtime_source)
        self.assertIn("capture_output=True", self.runtime_source)
        self.assertIn("PUBLIC_RESULT_KEYS", self.runtime_source)
        self.assertNotIn("print(completed.stdout", self.runtime_source)


if __name__ == "__main__":
    unittest.main()
