"""Tests for the MLI-02 dataset build orchestration DAG and control-plane runtime."""

from __future__ import annotations

import ast
from copy import deepcopy
from datetime import datetime
import hashlib
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
RUNNER_FIXTURE_PATH = REPOSITORY / "airflow" / "dags" / "fixtures" / "mock_build_runner.py"
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

    def test_production_fails_without_runner_and_without_mock(self):
        # Point 1: Missing runner in production must fail closed
        ctx = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_no_runner"}, self.env)
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_RUNNER_REQUIRED"):
            runtime.execute_step("wait_gold_published", ctx, None, self.env)

    def test_mock_pipeline_isolated_cannot_publish_official_registry(self):
        # Point 1: Mock pipeline is strictly for testing and cannot write to official registry
        req = {
            "conf": self.fixture["conf"],
            "run_id": "run_mock_test",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }
        summary = runtime.run(req, self.env)
        self.assertEqual("MOCK_COMPLETED", summary["status"])
        self.assertFalse(summary["colab_handoff"]["ready"])
        self.assertEqual("MOCK_NOT_ELIGIBLE_FOR_COLAB", summary["colab_handoff"]["status"])

        # Official registry must remain completely untouched
        official_reg_path = Path(self.env["MLI_DATASET_REGISTRY_ROOT"]) / f"{summary['dataset_id']}.json"
        self.assertFalse(official_reg_path.exists())

        # Official export root must remain completely untouched
        official_export_path = Path(self.env["ML_BUILD_EXPORT_ROOT"]) / f"dataset_id={summary['dataset_id']}"
        self.assertFalse(official_export_path.exists())

    def test_retry_isolates_export_attempts_without_mixing_parts(self):
        # Point 4: Different attempts produce isolated directories
        ctx = runtime.resolve_run_context({
            "conf": self.fixture["conf"],
            "run_id": "run_attempt_test",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }, self.env)
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

    def test_attempt_isolation_rejects_dirty_existing_directory(self):
        # Point 4: Attempt directory that already exists with files must be refused
        ctx = runtime.resolve_run_context({
            "conf": self.fixture["conf"],
            "run_id": "run_dirty_attempt",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }, self.env)
        upstream = None
        for step in runtime.STEPS[:8]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env, attempt=1)

        # Create attempt directory with dirty stray file
        dirty_dir = Path(ctx["staging_root"]) / "export_attempts" / "attempt_1"
        dirty_dir.mkdir(parents=True, exist_ok=True)
        (dirty_dir / "features-part-00001.parquet").write_bytes(b"STRAY_PART")

        with self.assertRaisesRegex(runtime.Mli02Error, "DS_ATTEMPT_DIRECTORY_EXISTS"):
            runtime.execute_step("export_parquet", ctx, upstream, self.env, attempt=1)

    def test_promote_excludes_stray_unindexed_attempt_parts(self):
        # Point 4: Promote must ONLY copy verified files listed in checksums.sha256
        ctx = runtime.resolve_run_context({
            "conf": self.fixture["conf"],
            "run_id": "run_promote_isolation",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }, self.env)
        upstream = None
        for step in runtime.STEPS[:9]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env, attempt=1)

        attempt_dir = Path(upstream["attempt_dir"])
        # Inject stray unindexed file in attempt directory
        (attempt_dir / "features-part-00099.parquet").write_bytes(b"UNINDEXED_STRAY_PART")

        # Step 10 promotes to mock_export
        step10 = runtime.execute_step("mark_dataset_exported", ctx, upstream, self.env, attempt=1)
        mock_export = Path(step10["export_artifact_uri"])
        self.assertTrue((mock_export / "features-part-00000.parquet").is_file())
        self.assertFalse((mock_export / "features-part-00099.parquet").exists())

    def test_failure_stops_before_dataset_exported_and_colab_not_ready(self):
        ctx = runtime.resolve_run_context({
            "conf": self.fixture["conf"],
            "run_id": "run_fail_test",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }, self.env)
        upstream = None
        for step in runtime.STEPS[:3]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env)

        # Corrupt Mc config to trigger failure at resolve_mc
        bad_ctx = deepcopy(ctx)
        bad_ctx["mock_pipeline"] = {"mc": {"mc_value": -1.0}}
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_MC_INVALID"):
            runtime.execute_step("resolve_mc", bad_ctx, upstream, self.env)

        # Confirm step 10 was never run
        step10_file = Path(ctx["staging_root"]) / "step_10_mark_dataset_exported.json"
        self.assertFalse(step10_file.exists())

    def test_readback_checksum_tampering_is_caught(self):
        ctx = runtime.resolve_run_context({
            "conf": self.fixture["conf"],
            "run_id": "run_tamper_test",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }, self.env)
        upstream = None
        for step in runtime.STEPS[:9]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env, attempt=1)

        # Tamper with the exported parquet before mark_dataset_exported
        attempt_dir = Path(upstream["attempt_dir"])
        (attempt_dir / "features-part-00000.parquet").write_bytes(b"TAMPERED_PARQUET_CONTENT")

        with self.assertRaisesRegex(runtime.Mli02Error, "DS_EXPORT_CHECKSUM_MISMATCH"):
            runtime.execute_step("mark_dataset_exported", ctx, upstream, self.env, attempt=1)

    def test_readback_count_mismatch_fails_gate(self):
        # Point 1 & 3: Manifest declares 142 rows but Parquet has 3 rows -> fails readback
        ctx = runtime.resolve_run_context({
            "conf": self.fixture["conf"],
            "run_id": "run_count_mismatch",
            "mock_pipeline": self.fixture["mock_pipeline"],
        }, self.env)
        upstream = None
        for step in runtime.STEPS[:9]:
            upstream = runtime.execute_step(step, ctx, upstream, self.env, attempt=1)

        # Overwrite parquet with only 3 rows while manifest claims 142
        attempt_dir = Path(upstream["attempt_dir"])
        part_path = attempt_dir / "features-part-00000.parquet"
        runtime._materialize_features_parquet(part_path, ctx, row_count=3)
        part_sha = hashlib.sha256(part_path.read_bytes()).hexdigest()
        (attempt_dir / "checksums.sha256").write_text(f"{part_sha}  features-part-00000.parquet\n", encoding="utf-8")

        with self.assertRaisesRegex(runtime.Mli02Error, "DS_COUNT_RECONCILIATION_FAILED:expected_142_got_3"):
            runtime.execute_step("mark_dataset_exported", ctx, upstream, self.env, attempt=1)

    def test_external_runner_receipt_validation_rejects_identity_mismatch(self):
        # Point 3: Validate receipt step, identity, attempt, snapshot
        ctx = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_receipt_test"}, self.env)

        # Test receipt with wrong dataset_id
        bad_report = {
            "step": "wait_gold_published",
            "status": "VERIFIED_PUBLISHED",
            "run_id": ctx["run_id"],
            "dataset_id": "wrong_dataset_id",
            "gold_snapshot_id": ctx["gold_publication"]["gold_snapshot_id"],
            "attempt": 1,
        }
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_RUNNER_RECEIPT_INVALID:wait_gold_published_dataset_id_mismatch"):
            runtime.validate_runner_receipt("wait_gold_published", bad_report, ctx, attempt=1)

        # Test receipt with wrong run_id
        bad_report2 = {
            "step": "wait_gold_published",
            "status": "VERIFIED_PUBLISHED",
            "run_id": "wrong_run_id",
            "dataset_id": ctx["dataset_id"],
            "gold_snapshot_id": ctx["gold_publication"]["gold_snapshot_id"],
            "attempt": 1,
        }
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_RUNNER_RECEIPT_INVALID:wait_gold_published_run_id_mismatch"):
            runtime.validate_runner_receipt("wait_gold_published", bad_report2, ctx, attempt=1)

    def test_leaf_gate_fails_if_lease_still_held(self):
        # Point 3: Leaf gate fails if lease is still held
        ctx = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": "run_lease_gate"}, self.env)

        # Construct a fake final result that did not release lease
        fake_final = {
            "status": "DATASET_EXPORTED",
            "dataset_id": ctx["dataset_id"],
            "run_id": ctx["run_id"],
            "gold_snapshot_id": ctx["gold_publication"]["gold_snapshot_id"],
            "export_artifact_uri": None,
        }
        with self.assertRaisesRegex(runtime.Mli02Error, "DS_EXPORT_GATE_FAILED"):
            runtime.mark_dataset_exported_summary(ctx, fake_final, self.env)

    def test_airflow_run_id_with_plus_and_colon_is_accepted(self):
        # Point 5: Standard Airflow run IDs with '+', ':', etc. must be accepted
        airflow_run_id = "manual__2026-10-17T00:00:00+00:00"
        ctx = runtime.resolve_run_context({"conf": self.fixture["conf"], "run_id": airflow_run_id}, self.env)
        self.assertEqual(airflow_run_id, ctx["run_id"])
        self.assertEqual(airflow_run_id, ctx["lease_owner"]["run_id"])

        # Staging directory on disk must be filesystem safe (no illegal colon on Windows)
        staging_dir = Path(ctx["staging_root"])
        self.assertTrue(staging_dir.is_dir())
        self.assertNotIn(":", staging_dir.name)
        runtime.lease("release", ctx["lease_owner"], self.env)

    def test_successful_full_run_with_external_runner_and_registry_handoff(self):
        # Points 1, 2, 3: Full production run using real external runner command protocol
        env = {
            **self.env,
            "ML_BUILD_RUNNER_COMMAND": f'"{sys.executable}" "{RUNNER_FIXTURE_PATH}"',
        }
        req = {
            "conf": self.fixture["conf"],
            "run_id": "run_prod_e2e_runner",
        }
        summary = runtime.run(req, env)

        self.assertEqual("DATASET_EXPORTED", summary["status"])
        self.assertTrue(summary["dataset_id"].startswith("ds_"))

        # Verify exported bundle on disk
        export_dir = Path(summary["export_artifact_uri"])
        self.assertTrue(export_dir.is_dir())
        self.assertTrue((export_dir / "features-part-00000.parquet").is_file())
        self.assertTrue((export_dir / "dataset_manifest.json").is_file())
        self.assertTrue((export_dir / "checksums.sha256").is_file())

        # Verify checksums do not have circular reference:
        # checksums.sha256 only hashes data parts
        checksums_lines = (export_dir / "checksums.sha256").read_text(encoding="utf-8").strip().splitlines()
        self.assertEqual(1, len(checksums_lines))
        self.assertIn("features-part-00000.parquet", checksums_lines[0])
        self.assertNotIn("dataset_manifest.json", (export_dir / "checksums.sha256").read_text(encoding="utf-8"))

        # Verify manifest registration in MLI_DATASET_REGISTRY_ROOT
        reg_root = Path(self.env["MLI_DATASET_REGISTRY_ROOT"])
        manifest_file = reg_root / f"{summary['dataset_id']}.json"
        self.assertTrue(manifest_file.is_file())
        manifest_data = json.loads(manifest_file.read_text(encoding="utf-8"))
        self.assertEqual("EXPORTED", manifest_data["dataset_status"])
        self.assertEqual(142, manifest_data["feature_row_count"])

        # Verify MLI-03 TrustedDatasetResolver compatibility:
        # export_manifest_ref can be resolved directly from registry root!
        ref = manifest_data["export_manifest_ref"]
        resolved_export_ref = (reg_root / ref).resolve()
        self.assertTrue(resolved_export_ref.is_file())
        export_sha = hashlib.sha256(resolved_export_ref.read_bytes()).hexdigest()
        self.assertEqual(manifest_data["export_manifest_sha256"], export_sha)

        # Verify Colab handoff parameters: manual operator required, no auto-trigger
        handoff = summary["colab_handoff"]
        self.assertEqual("MANUAL_OPERATOR_REQUIRED", handoff["status"])
        self.assertIs(True, handoff["ready"])
        self.assertIs(False, handoff["auto_triggered"])

        # Verify lease is released
        owner_file = Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json"
        self.assertFalse(owner_file.exists())


if __name__ == "__main__":
    unittest.main()
