"""Tests for the MLI-02 dataset build orchestration DAG and control-plane runtime."""

from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch

REPOSITORY = Path(__file__).parents[2]
DAG_PATH = REPOSITORY / "airflow" / "dags" / "mli_02_build_dataset.py"
sys.path.insert(0, str(DAG_PATH.parent))

import ml_dataset_runtime as runtime


class Reference:
    def __init__(self, task_id: str):
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
                    raise AssertionError(f"Duplicate task ID: {task_id}")
                self.tasks[task_id] = {
                    "options": {**self.options.get("default_args", {}), **options},
                    "args": args,
                    "kwargs": kwargs,
                }
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


class Mli02DagContractTest(unittest.TestCase):
    """Verify DAG structure, 10 task groups, edge order, and strict leaf behavior offline."""

    def setUp(self):
        self.sdk = OfflineSdk()
        airflow = ModuleType("airflow")
        sdk = ModuleType("airflow.sdk")
        sdk.dag = self.sdk.dag
        sdk.task = self.sdk.task
        sdk.task_group = self.sdk.task_group

        def no_task_execution():
            raise AssertionError("DAG import executed a task body")

        sdk.get_current_context = no_task_execution
        pendulum = ModuleType("pendulum")
        pendulum.datetime = lambda year, month, day, **kwargs: datetime(year, month, day)

        spec = importlib.util.spec_from_file_location("offline_mli_02_dag", DAG_PATH)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"airflow": airflow, "airflow.sdk": sdk, "pendulum": pendulum}):
            sys.path.insert(0, str(DAG_PATH.parent))
            try:
                spec.loader.exec_module(module)
            finally:
                sys.path.pop(0)

    def test_manual_paused_single_run_and_no_retries(self):
        self.assertEqual("mli_02_build_dataset", self.sdk.options["dag_id"])
        self.assertIsNone(self.sdk.options["schedule"])
        self.assertIs(False, self.sdk.options["catchup"])
        self.assertIs(True, self.sdk.options["is_paused_upon_creation"])
        self.assertEqual(1, self.sdk.options["max_active_runs"])
        self.assertEqual(1, self.sdk.options["max_active_tasks"])
        self.assertEqual(0, self.sdk.options["default_args"]["retries"])
        self.assertIn("mli-02", self.sdk.options["tags"])

    def test_ten_task_groups_in_exact_sequence(self):
        expected_groups = [
            "wait_gold_published",
            "resolve_gold_snapshot",
            "audit_gold_input",
            "resolve_mc",
            "select_mainshocks",
            "build_candidate_windows",
            "build_spacetime_features",
            "validate_feature_snapshot",
            "export_parquet",
            "mark_dataset_exported",
        ]
        self.assertEqual(expected_groups, self.sdk.groups)

        chain = [f"{grp}.execute_step" for grp in expected_groups]
        # All steps depend on resolve_run_context
        for task_id in chain:
            self.assertIn(("resolve_run_context", task_id), self.sdk.edges)
        # Sequential pipeline dependency: step[i] -> step[i+1]
        for upstream, downstream in zip(chain, chain[1:]):
            self.assertIn((upstream, downstream), self.sdk.edges)

    def test_sole_final_leaf_all_success(self):
        for task_info in self.sdk.tasks.values():
            self.assertEqual("all_success", task_info["options"]["trigger_rule"])
        parents = {p for p, _ in self.sdk.edges}
        leaves = set(self.sdk.tasks) - parents
        self.assertEqual({"mark_dataset_exported.contract_gate"}, leaves)

    def test_no_colab_external_service_triggers_in_source(self):
        source = DAG_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        self.assertIsInstance(tree, ast.Module)
        for forbidden in ("requests", "boto3", "google.colab", "colab_service", "all_done"):
            self.assertNotIn(forbidden, source)
        self.assertIn('"Asia/Ho_Chi_Minh"', source)


class Mli02RuntimeControlPlaneTest(unittest.TestCase):
    """Test runtime validation gates, retry idempotency, attempt isolation, and operator handoff."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        root = Path(self.temp_dir.name)
        self.env = {
            "ML_BUILD_STAGING_ROOT": str(root / "staging"),
            "ML_BUILD_EXPORT_ROOT": str(root / "export"),
            "MLI_DATASET_REGISTRY_ROOT": str(root / "registry"),
            "SOURCE_GUARD_ROOT": str(root / "guard"),
        }
        self.fixture_path = REPOSITORY / "airflow" / "dags" / "fixtures" / "mli_02_mock_v1.json"
        self.fixture = json.loads(self.fixture_path.read_text(encoding="utf-8"))

    def test_dag_fails_if_gold_not_published(self):
        bad_conf = deepcopy(self.fixture["conf"])
        bad_conf["publication_reference"]["published"] = False
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_GOLD_NOT_PUBLISHED"):
            runtime.resolve_run_context({"conf": bad_conf, "run_id": "test_run_01"}, self.env)

        bad_conf["publication_reference"]["published"] = True
        bad_conf["publication_reference"]["verify_status"] = "FAILED"
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_GOLD_NOT_PUBLISHED"):
            runtime.resolve_run_context({"conf": bad_conf, "run_id": "test_run_02"}, self.env)

    def test_dag_fails_if_dataset_config_invalid(self):
        bad_conf = deepcopy(self.fixture["conf"])
        bad_conf["period_end_utc_exclusive"] = "2019-01-01T00:00:00Z"
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_INVALID_PERIOD"):
            runtime.resolve_run_context({"conf": bad_conf, "run_id": "test_run_03"}, self.env)

        bad_conf2 = deepcopy(self.fixture["conf"])
        bad_conf2["dataset_config_version"] = "invalid version with spaces!"
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_INVALID_VERSION:dataset_config_version"):
            runtime.resolve_run_context({"conf": bad_conf2, "run_id": "test_run_04"}, self.env)

    def test_deterministic_dataset_id_and_identity_conflict_detection(self):
        ctx1 = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_01"}, self.env)
        runtime.lease("release", ctx1["lease_owner"], self.env)
        ctx2 = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_02"}, self.env)
        runtime.lease("release", ctx2["lease_owner"], self.env)
        self.assertEqual(ctx1["dataset_id"], ctx2["dataset_id"])
        self.assertTrue(ctx1["dataset_id"].startswith("ds_"))

        # Mutate registered manifest with differing payload digest
        reg_path = Path(self.env["MLI_DATASET_REGISTRY_ROOT"]) / f"{ctx1['dataset_id']}.json"
        reg_path.write_text(json.dumps({"identity_payload_sha256": "different_sha256"}), encoding="utf-8")

        with self.assertRaisesRegex(runtime.Mli02Error, "DS_IDENTITY_CONFLICT"):
            runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_03"}, self.env)

    def test_retry_isolates_export_attempts_without_mixing_parts(self):
        ctx = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_attempt_test"}, self.env)
        # Advance through step 8
        upstream = None
        for step in runtime.STEPS[:8]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env, attempt=1)

        # Attempt 1 export
        exp1 = runtime.execute_step("export_parquet", ctx, upstream, self.env, attempt=1)
        dir1 = Path(exp1["attempt_dir"])
        self.assertIn("attempt_1", str(dir1))
        self.assertTrue((dir1 / "features-part-00000.parquet").is_file())

        # Attempt 2 export (simulating a rerun attempt)
        exp2 = runtime.execute_step("export_parquet", ctx, upstream, self.env, attempt=2)
        dir2 = Path(exp2["attempt_dir"])
        self.assertIn("attempt_2", str(dir2))
        self.assertNotEqual(dir1, dir2)
        self.assertTrue((dir2 / "features-part-00000.parquet").is_file())

    def test_failure_stops_before_dataset_exported_and_colab_not_ready(self):
        ctx = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_fail_test"}, self.env)
        # Run steps up to audit
        upstream = None
        for step in runtime.STEPS[:3]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env)

        # Corrupt Mc config to trigger failure at resolve_mc
        bad_ctx = deepcopy(ctx)
        bad_ctx["mock_pipeline"] = {"mc": {"mc_value": -1.0}}
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_MC_INVALID"):
            runtime.execute_step("resolve_mc", bad_ctx, upstream, self.env)

        # Confirm dataset was never exported and registry has no entry
        reg_path = Path(self.env["MLI_DATASET_REGISTRY_ROOT"]) / f"{ctx['dataset_id']}.json"
        self.assertFalse(reg_path.exists())

        # Confirm step 10 was never run
        step10_file = Path(ctx["staging_root"]) / "step_10_mark_dataset_exported.json"
        self.assertFalse(step10_file.exists())

    def test_readback_checksum_tampering_is_caught(self):
        ctx = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_tamper_test"}, self.env)
        upstream = None
        for step in runtime.STEPS[:9]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env, attempt=1)

        # Tamper with the exported parquet before mark_dataset_exported
        attempt_dir = Path(upstream["attempt_dir"])
        (attempt_dir / "features-part-00000.parquet").write_bytes(b"TAMPERED_PARQUET_CONTENT")

        with self.assertRaisesRegex(runtime.Mli02Error, "DS_EXPORT_CHECKSUM_MISMATCH"):
            runtime.execute_step("mark_dataset_exported", ctx, upstream, self.env, attempt=1)

    def test_successful_full_run_and_manual_colab_handoff(self):
        req = {
            "conf": self.fixture["conf"],
            "run_id": "run_success_e2e",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }
        summary = runtime.run(req, self.env)

        self.assertEqual("DATASET_EXPORTED", summary["status"])
        self.assertEqual(self.fixture["conf"]["dataset_split"], summary["dataset_split"])
        self.assertTrue(summary["dataset_id"].startswith("ds_"))

        # Verify exported bundle on disk
        export_dir = Path(summary["export_artifact_uri"])
        self.assertTrue(export_dir.is_dir())
        self.assertTrue((export_dir / "features-part-00000.parquet").is_file())
        self.assertTrue((export_dir / "dataset_manifest.json").is_file())
        self.assertTrue((export_dir / "checksums.sha256").is_file())

        # Verify manifest registration
        reg_path = Path(summary["export_artifact_uri"]) / "dataset_manifest.json"
        manifest_data = json.loads(reg_path.read_text(encoding="utf-8"))
        self.assertEqual("EXPORTED", manifest_data["dataset_status"])
        self.assertEqual(summary["dataset_id"], manifest_data["dataset_id"])

        # Verify Colab handoff parameters: manual operator required, no auto-trigger
        handoff = summary["colab_handoff"]
        self.assertEqual("MANUAL_OPERATOR_REQUIRED", handoff["status"])
        self.assertIs(True, handoff["ready"])
        self.assertIs(False, handoff["auto_triggered"])
        self.assertIn("Operator should manually", handoff["notes"])


if __name__ == "__main__":
    unittest.main()
