"""Build the real DAG declaration with an offline SDK double, without executing tasks.

This verifies inferred edges, groups and strict leaf behavior. It is not evidence
of a scheduler run or a real Bronze/Silver/Gold publication.
"""

import ast
from datetime import datetime
import importlib.util
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import patch


REPOSITORY = Path(__file__).parents[2]
DAG_PATH = REPOSITORY / "airflow/dags/orc_01_etl_pipeline.py"


class Reference:
    def __init__(self, task_id):
        self.task_id = task_id


class OfflineSdk:
    def __init__(self):
        self.options = {}
        self.groups = []
        self.stack = []
        self.tasks = {}
        self.edges = set()

    def dag(self, **options):
        self.options = options
        return lambda function: function

    def task(self, **options):
        def decorate(function):
            def bind(*args, **kwargs):
                task_id = ".".join([*self.stack, options["task_id"]])
                if task_id in self.tasks:
                    raise AssertionError("Duplicate task ID")
                self.tasks[task_id] = {"options": {**self.options["default_args"], **options},
                                       "args": args, "kwargs": kwargs}
                for argument in [*args, *kwargs.values()]:
                    if isinstance(argument, Reference):
                        self.edges.add((argument.task_id, task_id))
                return Reference(task_id)
            return bind
        return decorate

    def task_group(self, **options):
        def decorate(function):
            def bind(*args, **kwargs):
                self.groups.append(options["group_id"])
                self.stack.append(options["group_id"])
                try:
                    return function(*args, **kwargs)
                finally:
                    self.stack.pop()
            return bind
        return decorate


class EtlDagContractTest(unittest.TestCase):
    def setUp(self):
        self.sdk = OfflineSdk()
        airflow = ModuleType("airflow")
        sdk = ModuleType("airflow.sdk")
        sdk.dag, sdk.task, sdk.task_group = self.sdk.dag, self.sdk.task, self.sdk.task_group
        def no_task_execution():
            raise AssertionError("DAG import executed a task body")
        sdk.get_current_context = no_task_execution
        pendulum = ModuleType("pendulum")
        pendulum.datetime = lambda year, month, day, **kwargs: datetime(year, month, day)
        spec = importlib.util.spec_from_file_location("offline_orc_01_dag", DAG_PATH)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"airflow": airflow, "airflow.sdk": sdk, "pendulum": pendulum}):
            # Runtime module is importable on a fresh host as well as unittest discovery.
            sys.path.insert(0, str(DAG_PATH.parent))
            try:
                spec.loader.exec_module(module)
            finally:
                sys.path.pop(0)

    def test_manual_paused_bounded_and_no_automatic_retries(self):
        self.assertEqual("orc_01_etl_pipeline", self.sdk.options["dag_id"])
        self.assertIsNone(self.sdk.options["schedule"])
        self.assertIs(False, self.sdk.options["catchup"])
        self.assertIs(True, self.sdk.options["is_paused_upon_creation"])
        self.assertEqual(1, self.sdk.options["max_active_runs"])
        self.assertEqual(1, self.sdk.options["max_active_tasks"])
        self.assertEqual(0, self.sdk.options["default_args"]["retries"])

    def test_six_groups_have_exact_chain_and_context_dependency(self):
        self.assertEqual(["source_readiness", "bronze", "silver", "gold", "verification", "publication"], self.sdk.groups)
        chain = ["source_readiness.execute_adapter", "bronze.execute_adapter", "silver.execute_adapter",
                 "gold.execute_adapter", "verification.execute_adapter", "publication.execute_adapter", "publication.contract_gate"]
        expected = {("resolve_run_context", task_id) for task_id in chain}
        expected.update(zip(chain, chain[1:]))
        self.assertEqual(expected, self.sdk.edges)
        self.assertEqual({"resolve_run_context", *chain}, set(self.sdk.tasks))
        self.assertEqual(["readiness", "bronze", "silver", "gold", "verify", "publish"],
                         [self.sdk.tasks[task_id]["args"][0] for task_id in chain[:-1]])

    def test_all_success_leaf_cannot_hide_upstream_failure(self):
        for task in self.sdk.tasks.values():
            self.assertEqual("all_success", task["options"]["trigger_rule"])
        parents = {parent for parent, child in self.sdk.edges}
        self.assertEqual({"publication.contract_gate"}, set(self.sdk.tasks) - parents)

    def test_import_is_metadata_only_and_no_colab_ml_or_live_calls(self):
        source = DAG_PATH.read_text()
        tree = ast.parse(source)
        calls = [node.value.func.id for node in tree.body if isinstance(node, ast.Expr)
                 and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)]
        self.assertEqual(["etl_pipeline"], calls)
        for unwanted in ("requests", "boto3", "zipfile", "subprocess", "Colab", "ml_dataset", "all_done"):
            self.assertNotIn(unwanted, source)
        self.assertIn('"Asia/Ho_Chi_Minh"', source)

    def test_optional_real_adapter_configuration_and_fixture_mount(self):
        compose = (REPOSITORY / "compose.yaml").read_text()
        example = (REPOSITORY / ".env.example").read_text()
        for key in ("ETL_PHASE_RUNNER_COMMAND", "ETL_PHASE_RUNNER_TIMEOUT_SECONDS"):
            self.assertIn(key + ":", compose)
            self.assertIn(key + "=", example)
        self.assertIn("ETL_STAGING_ROOT: /opt/pipeline/staging/etl", compose)
        self.assertIn("source: ./airflow/dags", compose)
        self.assertIn("read_only: true", compose)
        self.assertTrue((DAG_PATH.parent / "fixtures/orc_01_mock_v1.json").is_file())


if __name__ == "__main__":
    unittest.main()
