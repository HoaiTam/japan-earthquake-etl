"""Static tests for the AFL-01 smoke DAG contract."""

from __future__ import annotations

import ast
from pathlib import Path
import unittest


class SmokeDagContractTest(unittest.TestCase):
    """Keep the foundation DAG deterministic and safe to run locally."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.dag_path = Path(__file__).parents[1] / "dags" / "afl_01_smoke.py"
        cls.source = cls.dag_path.read_text(encoding="utf-8")
        cls.tree = ast.parse(cls.source, filename=str(cls.dag_path))

    def test_dag_file_is_valid_python(self) -> None:
        self.assertIsInstance(self.tree, ast.Module)

    def test_manual_smoke_dag_is_enabled_without_catchup(self) -> None:
        self.assertIn('dag_id="afl_01_smoke"', self.source)
        self.assertIn("schedule=None", self.source)
        self.assertIn("catchup=False", self.source)
        self.assertIn("is_paused_upon_creation=False", self.source)
        self.assertIn("max_active_runs=1", self.source)

    def test_task_logs_run_context_without_external_calls(self) -> None:
        self.assertIn("get_current_context", self.source)
        self.assertIn("run_id", self.source)
        self.assertNotIn("requests.", self.source)
        self.assertNotIn("boto3", self.source)


if __name__ == "__main__":
    unittest.main()
