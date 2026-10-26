"""Protocol tests use a fake Java boundary; they are not runtime acceptance."""
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import ml_dataset_real as real
import ml_dataset_runtime as dispatch


class RealDatasetTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.env = {"ML_BUILD_MODE": "real", "ML_BUILD_NAMESPACE": "iceberg.mli02_ml_qa_unit",
                    "ML_BUILD_RUNNER_SHA256": "a" * 64, "SOURCE_GUARD_ROOT": str(self.root / "guard"),
                    "ML_BUILD_STAGING_ROOT": str(self.root / "build"), "ML_BUILD_EXPORT_ROOT": str(self.root / "export"),
                    "MLI_DATASET_REGISTRY_ROOT": str(self.root / "registry")}
        fixture = Path(__file__).parents[1] / "dags/fixtures/mli_02_mock_v1.json"
        self.raw = {"conf": json.loads(fixture.read_text())["conf"], "dag_id": real.DAG,
                    "run_id": "manual__2026-10-24T01:00:00+00:00"}
        self.raw["conf"].pop("mock_pipeline", None)
        self.pub = dispatch.verify_gold_published(self.raw["conf"]["publication_reference"])
        self.pub["gold_snapshot_id"] = 9079127511755137392
        self.raw["conf"]["publication_reference"]["gold_snapshot_id"] = self.pub["gold_snapshot_id"]
        self.plan = {"dataset_id": "ds_" + "b" * 64, "identity_payload_sha256": "b" * 64, "gold_publication": self.pub}
        self.calls = []
        p = patch.object(real, "prepared", return_value=([], 10)); p.start(); self.addCleanup(p.stop)
        p = patch.object(real, "invoke", side_effect=self.runner); p.start(); self.addCleanup(p.stop)

    def runner(self, req, env):
        self.calls.append(req)
        if req["step"] == "plan": return deepcopy(self.plan)
        step = req["step"]
        result = {"contract_version": req["contract_version"], "step": step, "run_id": req["run_id"],
                  "dataset_id": self.plan["dataset_id"], "identity_payload_sha256": self.plan["identity_payload_sha256"],
                  "gold_snapshot_id": self.pub["gold_snapshot_id"], "attempt": req["attempt"],
                  "plan_sha256": hashlib.sha256(real.payload(self.plan)).hexdigest(), "status": real.STATUSES[real.STEPS.index(step)]}
        if step == real.STEPS[-1]:
            bundle = Path(req["export_root"]) / ("dataset_id=" + req["dataset_id"]); bundle.mkdir(parents=True, exist_ok=True)
            trusted = Path(req["registry_root"]) / (req["dataset_id"] + ".json")
            manifest = {"dataset_id": req["dataset_id"], "dataset_status": "EXPORTED", "gold_snapshot_id": self.pub["gold_snapshot_id"]}
            real.immutable(trusted, manifest); real.immutable(bundle / "dataset_manifest.json", manifest)
            (bundle / "features-part-00000.parquet").write_bytes(b"protocol-test-only-not-real-parquet")
            (bundle / "export-checksums.sha256").write_text(real.sha(bundle / "features-part-00000.parquet") + "  features-part-00000.parquet\n")
            (bundle / "checksums.sha256").write_text("".join(real.sha(bundle / n) + "  " + n + "\n" for n in (
                "dataset_manifest.json", "export-checksums.sha256", "features-part-00000.parquet")))
            result.update(dataset_manifest_sha256=real.sha(trusted), bundle_manifest_sha256=real.sha(bundle / "dataset_manifest.json"),
                          checksums_sha256=real.sha(bundle / "checksums.sha256"), trusted_handoff_verified=True,
                          feature_row_count=10, mainshock_count=2, invalid_feature_count=0, quality_gate_status="PASSED")
        return result

    def context(self): return dispatch.resolve_run_context(self.raw, self.env)
    def full(self):
        ctx, up = self.context(), None
        for step in real.STEPS: up = dispatch.execute_step(step, ctx, up, self.env)
        return ctx, up

    def test_java_authority_bigint_and_actual_owner(self):
        ctx = self.context()
        self.assertEqual(self.plan["dataset_id"], ctx["dataset_id"])
        self.assertEqual(9079127511755137392, ctx["plan"]["gold_publication"]["gold_snapshot_id"])
        self.assertNotIn(":", ctx["build_run_id"])
        self.assertEqual(self.raw["run_id"], real.read(self.root / "guard/owner.json")["run_id"])

    def collection_conf(self):
        filters=self.raw["conf"]["filter_config"]
        filters["window_coverage_policy"]="dat03-strict-split-v1"
        filters["collection_export"]={"version":"dat03-collection-export-v1",
                                      "layout_limits":{"mainshocks_per_shard":128,"rows_per_shard":100000,"max_shards":64}}

    def test_collection_version_is_derived_from_validated_identity_config(self):
        self.collection_conf();ctx=self.context()
        self.assertEqual(real.COLLECTION_VERSION,ctx["contract_version"])
        self.assertEqual(real.COLLECTION_VERSION,ctx["runtime_protocol"])
        first=dispatch.execute_step(real.STEPS[0],ctx,None,self.env)
        self.assertEqual(real.COLLECTION_VERSION,first["contract_version"])
        changed={**ctx,"runtime_protocol":real.VERSION}
        with self.assertRaisesRegex(RuntimeError,"REAL_CONTEXT_REQUIRED"):
            dispatch.execute_step(real.STEPS[1],changed,first,self.env)

    def test_collection_rejects_boolean_limits_or_unknown_version_before_lease(self):
        self.collection_conf()
        layout=self.raw["conf"]["filter_config"]["collection_export"]
        layout["layout_limits"]["rows_per_shard"]=True
        with self.assertRaisesRegex(RuntimeError,"COLLECTION_CONFIG_INVALID"):self.context()
        layout["layout_limits"]["rows_per_shard"]=100000;layout["version"]="unknown"
        with self.assertRaisesRegex(RuntimeError,"COLLECTION_CONFIG_INVALID"):self.context()
        self.assertFalse((self.root/"guard/owner.json").exists())

    def test_staged_runner_is_exact_and_does_not_change_default(self):
        self.assertEqual(real.JAR,real.runner(self.env))
        path="/opt/pipeline/staging/dat03-ml-export/"+"1"*32+"/runner.jar"
        self.assertEqual(Path(path),real.runner({**self.env,"ML_BUILD_RUNNER_JAR":path}))
        for bad in ("/tmp/runner.jar","/opt/spark/jobs/runner.jar",path.replace("runner.jar","other.jar")):
            with self.assertRaisesRegex(RuntimeError,"EXACT_STAGED_RUNNER_REQUIRED|UNSAFE_PATH"):real.runner({**self.env,"ML_BUILD_RUNNER_JAR":bad})

    def test_invalid_config_no_lease_no_java(self):
        for key, value in (("dataset_split", "invalid"), ("mock_pipeline", True)):
            bad = deepcopy(self.raw); bad["conf"][key] = value
            with self.assertRaises(RuntimeError): dispatch.resolve_run_context(bad, self.env)
        self.assertFalse((self.root / "guard/owner.json").exists()); self.assertEqual([], self.calls)

    def test_missing_or_unknown_mode_never_falls_back_to_mock(self):
        env = dict(self.env); env.pop("ML_BUILD_MODE")
        self.assertEqual(real.VERSION, dispatch.resolve_run_context(self.raw, env)["runtime_protocol"])
        with self.assertRaisesRegex(RuntimeError, "REAL_MODE_REQUIRED"):
            dispatch.resolve_run_context(self.raw, {**self.env, "ML_BUILD_MODE": "typo"})
        with self.assertRaisesRegex(RuntimeError, "REAL_CONTEXT_REQUIRED"):
            dispatch.execute_step(real.STEPS[0], {}, None, self.env)
        with self.assertRaisesRegex(RuntimeError, "REAL_CONTEXT_REQUIRED"):
            dispatch.mark_dataset_exported_summary({}, {}, self.env)

    def test_unpublished_input_no_lease(self):
        self.raw["conf"]["publication_reference"]["published"] = False
        with self.assertRaises(RuntimeError): self.context()
        self.assertFalse((self.root / "guard/owner.json").exists())

    def test_private_conf_rejected_before_lease(self):
        self.raw["conf"]["password"] = "never-log-this"
        with self.assertRaisesRegex(RuntimeError, "UNSAFE_CONFIG_KEY"): self.context()
        self.assertFalse((self.root / "guard/owner.json").exists())

    def test_resolve_retry_and_changed_config(self):
        ctx = self.context(); self.assertEqual(ctx, self.context()); self.assertEqual(1, len(self.calls))
        self.raw["conf"]["feature_version"] = "changed"
        with self.assertRaisesRegex(RuntimeError, "CONTEXT_CHANGED"): self.context()

    def test_foreign_owner_never_taken(self):
        owner = {"dag_id": "other", "run_id": "foreign"}; real.lease("acquire", owner, self.env)
        with self.assertRaises(RuntimeError): self.context()
        self.assertEqual(owner, real.read(self.root / "guard/owner.json"))

    def test_order_and_upstream(self):
        ctx = self.context()
        with self.assertRaises(RuntimeError): real.execute(real.STEPS[2], ctx, {}, self.env, 1)
        up = real.execute(real.STEPS[0], ctx, None, self.env, 1)
        with self.assertRaisesRegex(RuntimeError, "UPSTREAM_CHANGED"):
            real.execute(real.STEPS[1], ctx, {**up, "dataset_id": "wrong"}, self.env, 1)

    def test_wrong_status_attempt_plan_snapshot_fail_closed(self):
        ctx = self.context()
        for key, value in (("status", "MOCK_COMPLETED"), ("attempt", 99), ("plan_sha256", "f" * 64), ("gold_snapshot_id", 1)):
            answer = self.runner({**ctx, "step": real.STEPS[0], "attempt": 1}, self.env); answer[key] = value
            with patch.object(real, "invoke", return_value=answer), self.assertRaises(RuntimeError):
                real.execute(real.STEPS[0], ctx, None, self.env, 1)
            self.assertFalse((Path(ctx["run_root"]) / "receipts/wait_gold_published.json").exists())

    def test_success_retry_and_closed_colab_gate(self):
        ctx, final = self.full(); result = dispatch.mark_dataset_exported_summary(ctx, final, self.env)
        self.assertEqual("DATASET_EXPORTED", result["status"]); self.assertTrue(result["lease_released"])
        self.assertFalse(result["colab_handoff"]["ready"]); self.assertFalse(result["colab_handoff"]["auto_triggered"])
        self.assertEqual(final, real.execute(real.STEPS[-1], ctx, None, self.env, 2))
        self.assertFalse((self.root / "guard/owner.json").exists())

    def test_corrupt_bundle_blocks_leaf(self):
        ctx, final = self.full()
        (Path(ctx["export_root"]) / ("dataset_id=" + ctx["dataset_id"]) / "features-part-00000.parquet").write_text("corrupt")
        with self.assertRaisesRegex(RuntimeError, "BUNDLE_CHANGED"): real.summary(ctx, final, self.env)

    def test_read_only_collector_never_creates_missing_summary(self):
        ctx, final = self.full()
        target = Path(ctx["run_root"]) / "run-summary.json"
        with self.assertRaisesRegex(RuntimeError, "BOUNDED_METADATA_REQUIRED"):
            real.summary(ctx, final, self.env, persist=False)
        self.assertFalse(target.exists())
        result = real.summary(ctx, final, self.env)
        self.assertEqual(result, real.summary(ctx, final, self.env, persist=False))

    def test_oversized_config_rejected_before_lease(self):
        self.raw["conf"]["filter_config"]["too_large"] = "x" * 65536
        with self.assertRaisesRegex(RuntimeError, "BOUNDED_METADATA_REQUIRED"): self.context()
        self.assertFalse((self.root / "guard/owner.json").exists())

    def test_leaf_requires_release_receipt_and_no_own_lease(self):
        ctx, final = self.full(); real.lease("acquire", ctx["lease_owner"], self.env)
        with self.assertRaisesRegex(RuntimeError, "STILL_HELD"): real.summary(ctx, final, self.env)
        real.lease("release", ctx["lease_owner"], self.env); (Path(ctx["run_root"]) / "lease-release.json").unlink()
        with self.assertRaises(RuntimeError): real.summary(ctx, final, self.env)

    def test_changed_runner_rejected(self):
        ctx = self.context()
        with self.assertRaisesRegex(RuntimeError, "DEPLOYMENT_CHANGED"):
            real.execute(real.STEPS[0], ctx, None, {**self.env, "ML_BUILD_RUNNER_SHA256": "f" * 64}, 1)

    def test_runner_failure_retains_owner(self):
        ctx = self.context()
        with patch.object(real, "invoke", side_effect=RuntimeError("MLI02_RUNNER_FAILED_LEASE_RETAINED")), self.assertRaises(RuntimeError):
            real.execute(real.STEPS[0], ctx, None, self.env, 1)
        self.assertEqual(ctx["lease_owner"], real.read(self.root / "guard/owner.json")); self.assertFalse(Path(ctx["export_root"]).exists())

    def test_symlink_immutable_guards(self):
        real.immutable(self.root / "one.json", {"id": 1})
        with self.assertRaisesRegex(RuntimeError, "IMMUTABLE_CONFLICT"): real.immutable(self.root / "one.json", {"id": 2})
        (self.root / "alias").symlink_to(self.root, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "UNSAFE_PATH"): real.safe(self.root / "alias/one.json")

    def test_timeout_preserves_private_logs_and_owner(self):
        ctx = self.context()
        from importlib import util
        spec = util.spec_from_file_location("fresh_real", Path(real.__file__)); module = util.module_from_spec(spec); spec.loader.exec_module(module)
        with patch.object(module, "prepared", return_value=([], 1)), patch.object(module, "java_identity", side_effect=lambda env, _: env), \
                patch.object(module.subprocess, "run", side_effect=subprocess.TimeoutExpired("spark", 1)):
            with self.assertRaisesRegex(RuntimeError, "TIMEOUT_LEASE_RETAINED"):
                module.invoke({**ctx, "step": real.STEPS[0], "attempt": 1}, self.env)
        logs = list(Path(ctx["run_root"]).glob("attempts/*/*.private")); self.assertEqual(2, len(logs))
        self.assertTrue(all(p.stat().st_mode & 0o777 == 0o600 for p in logs)); self.assertTrue((self.root / "guard/owner.json").exists())

class ReadOnlyCollectorTest(unittest.TestCase):
    def test_sql_writes_are_rejected_without_network(self):
        from ml_dataset_build_acceptance import query
        with patch("urllib.request.urlopen") as call:
            for sql in ("UPDATE x SET y=1", "DELETE FROM x", "CREATE TABLE x(y int)"):
                with self.assertRaisesRegex(RuntimeError, "READ_ONLY_SQL_REQUIRED"): query(sql, {})
            call.assert_not_called()

    def test_next_uri_cannot_escape_configured_trino(self):
        from ml_dataset_build_acceptance import query
        page = io.BytesIO(b'{"nextUri":"https://outside.invalid/steal"}')
        with patch("urllib.request.urlopen", return_value=page), self.assertRaisesRegex(RuntimeError, "ENDPOINT_CHANGED"):
            query("SELECT count(*) FROM x", {"TRINO_HOST": "trino", "TRINO_INTERNAL_PORT": "8080"})

    def test_remote_error_does_not_leak_server_message(self):
        from ml_dataset_build_acceptance import query
        page = io.BytesIO(b'{"error":{"message":"sensitive-server-detail"}}')
        with patch("urllib.request.urlopen", return_value=page), self.assertRaisesRegex(RuntimeError, "READBACK_SQL_FAILED") as error:
            query("SELECT count(*) FROM x", {"TRINO_HOST": "trino", "TRINO_INTERNAL_PORT": "8080"})
        self.assertNotIn("sensitive-server-detail", str(error.exception))

    def test_bigint_readback_stays_integer(self):
        from ml_dataset_build_acceptance import query
        page = io.BytesIO(b'{"data":[[9079127511755137392]]}')
        with patch("urllib.request.urlopen", return_value=page):
            self.assertEqual([[9079127511755137392]], query("SELECT snapshot_id FROM x", {"TRINO_HOST": "trino", "TRINO_INTERNAL_PORT": "8080"}))


class PreparedRuntimeTest(unittest.TestCase):
    def test_missing_pin_wrong_bytes_wrong_dependency_and_timeout_fail_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); jar = root / "runner.jar"; jar.write_bytes(b"fixture")
            spark = root / "spark-submit"; spark.touch(); jars = root / "iceberg"; jars.mkdir()
            for name in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar"): (jars / name).touch()
            env = {"ML_BUILD_NAMESPACE": "iceberg.ml", "ML_BUILD_RUNNER_SHA256": hashlib.sha256(b"fixture").hexdigest()}
            with patch.object(real, "JAR", jar), patch.object(real, "SPARK", spark), patch.object(real, "ICEBERG", jars):
                self.assertEqual(2, len(real.prepared(env)[0]))
                for changes in ({"ML_BUILD_RUNNER_SHA256": ""}, {"ML_BUILD_RUNNER_SHA256": "a" * 64},
                                {"ML_BUILD_RUNNER_TIMEOUT_SECONDS": "0"}, {"ML_BUILD_MODE": "legacy-test"}):
                    with self.assertRaises(RuntimeError): real.prepared({**env, **changes})
                (jars / "unknown.jar").touch()
                with self.assertRaisesRegex(RuntimeError, "ICEBERG_NOT_PREPARED"): real.prepared(env)

if __name__ == "__main__": unittest.main()
