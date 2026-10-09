"""Scoped adapter doubles only; not evidence of actual Silver/Gold publication."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import backfill_observability as telemetry
import backfill_runtime as runtime
import etl_pipeline_runtime as etl
import run_observability as obs
from source_run_guard import lease
from test_backfill_runtime import base, adapter_receipt


class BackfillObservabilityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.env = {"DATA_BUCKET": "lake", "BRONZE_PREFIX": "bronze", "CONFIG_VERSION": "1",
                    "RUN_SUMMARY_ROOT": self.temp.name + "/summary",
                    "BACKFILL_STAGING_ROOT": self.temp.name + "/backfill",
                    "SOURCE_GUARD_ROOT": self.temp.name + "/guard",
                    "BACKFILL_ETL_RUNNER_COMMAND": "adapter-test-double"}
        self.run_id = "manual:observability:1"

    def resolve(self, action="reuse", run_id=None, conf=None):
        self.run_id = run_id or self.run_id
        self.plan = telemetry.resolve_observed({"run_id": self.run_id,
            "dag_run": {"conf": conf or base(action)}}, self.env)
        self.context = telemetry.context(self.plan, self.run_id) if not self.plan["preview"] else None
        return self.plan

    def acquire(self):
        return telemetry.source_lease(self.plan, self.run_id, "acquire", self.env)

    def release(self):
        return telemetry.source_lease(self.plan, self.run_id, "release", self.env)

    def summary(self):
        return obs.read_summary(self.context, self.env)

    def adapter(self, command, request, target, env):
        if "phase" not in request:
            rows = [{**pin, "record_count_estimate": 2,
                     "raw_object_uri": pin["manifest_uri"].replace("manifest.json", "response.geojson"
                        if pin["source_system"] == "USGS" else "archive.zip")} for pin in request["bronze_inputs"]]
            return {"contract_version": "orc-03-v1", "scope_sha256": self.plan["scope_sha256"],
                    "status": "BronzeVerified", "verified": True, "bronze_inputs": deepcopy(request["bronze_inputs"]),
                    "observability": {"version": obs.VERSION, "bronze_inputs": rows}}
        result = adapter_receipt(self.plan, request["phase"], request["upstream"])
        if request["phase"] in etl.load_mock_fixture()["observability"]:
            result["etl_receipt"]["observability"] = deepcopy(etl.load_mock_fixture()["observability"][request["phase"]])
        return result

    def execute(self, adapter=None):
        with patch.object(runtime, "invoke", side_effect=adapter or self.adapter):
            return runtime.execute(self.plan, self.env, telemetry_context=self.context)

    def test_preview_and_callback_create_no_persistent_files(self):
        conf = base("reuse"); conf["preview"] = True
        with patch("subprocess.run", side_effect=AssertionError("process")):
            self.resolve(conf=conf)
            self.assertFalse(runtime.preview_summary(self.plan)["published"])
            obs.dag_failure_summary({"dag_id": "orc_03_backfill", "run_id": self.run_id}, self.env)
        self.assertEqual([], list(Path(self.temp.name).iterdir()))

    def test_reuse_count_readback_lineage_and_release_gate(self):
        self.resolve(); self.acquire(); result = self.execute()
        self.assertEqual("RUNNING", self.summary()["status"])
        released = self.release()
        telemetry.complete(self.plan, self.run_id, result, released, self.env)
        summary = self.summary()
        self.assertEqual("BronzeVerified", summary["status"]); self.assertFalse(summary["published"])
        self.assertEqual(self.plan["scope_sha256"], summary["scope_sha256"])
        for source in summary["sources"]:
            self.assertEqual(2, source["counts"]["input"])
            self.assertIsNone(source["counts"]["fetched"]); self.assertIsNone(source["counts"]["parsed"])
            self.assertEqual(1, len(source["bronze_pins"]))
        self.assertEqual(["old-release"], summary["sources"][0]["catalog_releases"])

    def test_preview_cannot_invalidate_existing_successful_operation(self):
        self.resolve(); self.acquire(); result = self.execute(); self.release()
        target = runtime.root(self.plan, self.env) / "run_summary.json"
        before = target.read_bytes()
        conf = base("reuse"); conf["preview"] = True
        plan = runtime.resolve({"dag_run": {"conf": conf}}, self.env)
        with self.assertRaisesRegex(runtime.BackfillError, "PREVIEW_CANNOT_EXECUTE"):
            runtime.execute(plan, self.env)
        self.assertEqual(before, target.read_bytes())
        self.assertEqual("BronzeVerified", result["status"])

    def test_execution_without_held_lease_fails_before_source_or_staging(self):
        self.resolve()
        with patch.object(runtime, "execute_sources") as source:
            with self.assertRaisesRegex(obs.ObservabilityError, "SOURCE_LEASE_BUSY"):
                runtime.execute(self.plan, self.env, telemetry_context=self.context)
        source.assert_not_called()
        self.assertFalse(Path(self.env["BACKFILL_STAGING_ROOT"]).exists())

    def test_completion_requires_actual_cleanup_phase(self):
        self.resolve(); self.acquire(); result = self.execute()
        with self.assertRaisesRegex(obs.ObservabilityError, "MISSING_UPSTREAM_GATE"):
            telemetry.complete(self.plan, self.run_id, result, {"status": "RELEASED"}, self.env)
        self.assertFalse(self.summary()["published"]); self.release()

    def test_legacy_verifier_unknown_counts_not_manifest_length_or_zero(self):
        self.resolve(); self.acquire()
        def legacy(*args):
            result = self.adapter(*args); result.pop("observability"); return result
        result = self.execute(legacy)
        telemetry.complete(self.plan, self.run_id, result, self.release(), self.env)
        for source in self.summary()["sources"]:
            self.assertIsNone(source["counts"]["input"])
            self.assertEqual(1, len(source["bronze_pins"]))

    def test_zero_verified_count_is_not_missing(self):
        self.resolve(); self.acquire()
        def empty(*args):
            result = self.adapter(*args)
            for row in result["observability"]["bronze_inputs"]: row["record_count_estimate"] = 0
            return result
        self.execute(empty)
        self.assertEqual([0, 0], [row["counts"]["input"] for row in self.summary()["sources"]])
        self.release()

    def test_invalid_readback_count_pin_version_or_secret_is_rejected(self):
        for mutation in ("null", "count", "pin", "version", "secret"):
            self.resolve(); self.acquire()
            def invalid(*args):
                result = self.adapter(*args); report = result["observability"]
                if mutation == "null": result["observability"] = None
                if mutation == "count": report["bronze_inputs"][0]["record_count_estimate"] = True
                if mutation == "pin": report["bronze_inputs"][0]["manifest_sha256"] = "f" * 64
                if mutation == "version": report["version"] = "other"
                if mutation == "secret": report["bronze_inputs"][0]["token"] = "secret-sentinel"
                return result
            with self.subTest(mutation=mutation), self.assertRaises(obs.ObservabilityError): self.execute(invalid)
            self.assertEqual("source", self.summary()["failed_phase"])
            self.release()
        self.assertNotIn("secret-sentinel", "".join(path.read_text() for path in Path(self.env["RUN_SUMMARY_ROOT"]).rglob("*.json")))

    def test_source_failure_safe_reason_and_foreign_lease_never_released(self):
        self.resolve(); other = {"dag_id": "orc_02_daily_sources", "run_id": "foreign"}
        lease("acquire", other, self.env)
        with self.assertRaisesRegex(obs.ObservabilityError, "SOURCE_LEASE_BUSY"): self.acquire()
        with self.assertRaisesRegex(obs.ObservabilityError, "SOURCE_LEASE_NOT_RELEASED"): self.release()
        self.assertFalse(self.summary()["published"])
        self.assertEqual("HELD", lease("assert", other, self.env)["status"])
        lease("release", other, self.env)

    def test_unknown_exception_redacted_in_logs_and_persistence(self):
        self.resolve(); self.acquire()
        with self.assertLogs(obs.LOGGER, level="INFO") as logs, patch.object(runtime, "invoke",
                side_effect=RuntimeError("secret-sentinel https://user:password@private")):
            with self.assertRaisesRegex(obs.ObservabilityError, "PIPELINE_OPERATION_FAILED"):
                runtime.execute(self.plan, self.env, telemetry_context=self.context)
        self.assertNotIn("secret-sentinel", " ".join(logs.output))
        self.assertEqual("PIPELINE_OPERATION_FAILED", self.summary()["reason"])
        self.release()

    def test_missing_scoped_adapter_persists_failed_without_backfill_staging(self):
        self.resolve("reprocess"); self.acquire(); self.env["BACKFILL_ETL_RUNNER_COMMAND"] = ""
        with patch.object(runtime, "execute_sources") as source:
            with self.assertRaisesRegex(obs.ObservabilityError, "SCOPED_ETL_ADAPTER_NOT_CONFIGURED"): self.execute()
        source.assert_not_called()
        self.assertFalse(Path(self.env["BACKFILL_STAGING_ROOT"]).exists())
        self.assertEqual("source", self.summary()["failed_phase"]); self.release()

    def test_scoped_phase_counts_not_double_counted_and_publication_waits_cleanup(self):
        self.resolve("reprocess"); self.acquire(); result = self.execute()
        self.assertEqual("RUNNING", self.summary()["status"]); self.assertFalse(self.summary()["published"])
        telemetry.complete(self.plan, self.run_id, result, self.release(), self.env)
        summary = self.summary()
        self.assertEqual("Published", summary["status"])
        self.assertEqual(4, summary["gold_counts"]["published"])
        self.assertEqual([2, 2], [row["counts"]["input"] for row in summary["sources"]])
        # This is a trusted protocol double, NEVER live E2E/Gold evidence.

    def test_each_scoped_phase_failure_stops_later_phases_and_never_publishes(self):
        for failed in etl.PHASES:
            self.resolve("reprocess"); self.acquire(); called = []
            def adapter(*args):
                result = self.adapter(*args)
                if "etl_receipt" in result:
                    phase = result["etl_receipt"]["phase"]; called.append(phase)
                    if phase == failed: result["scope_receipt"]["outside_scope_unchanged"] = False
                return result
            with self.subTest(phase=failed), self.assertRaisesRegex(obs.ObservabilityError, "SCOPED_WRITE_GATE_FAILED"):
                self.execute(adapter)
            self.assertEqual(failed, self.summary()["failed_phase"])
            self.assertEqual(list(etl.PHASES[:etl.PHASES.index(failed) + 1]), called)
            self.release(); self.assertFalse(self.summary()["published"])

    def test_changed_verified_bronze_count_fails_before_silver(self):
        self.resolve("reprocess"); self.acquire()
        def adapter(*args):
            result = self.adapter(*args)
            if "etl_receipt" in result and result["etl_receipt"]["phase"] == "bronze":
                result["etl_receipt"]["artifacts"]["bronze_inputs"][0]["record_count_estimate"] = 3
            return result
        with self.assertRaisesRegex(obs.ObservabilityError, "BRONZE_INPUT_CHANGED"): self.execute(adapter)
        self.assertEqual("bronze", self.summary()["failed_phase"]); self.release()

    def test_completion_cannot_accept_forged_execution_summary(self):
        self.resolve("reprocess"); self.acquire(); result = self.execute(); released = self.release()
        result["publication"]["publication_uri"] = "s3://lake/other/publication.json"
        runtime.jma._atomic_json(runtime.root(self.plan, self.env) / "run_summary.json", result)
        with self.assertRaisesRegex(obs.ObservabilityError, "PUBLICATION_INPUT_CHANGED"):
            telemetry.complete(self.plan, self.run_id, result, released, self.env)
        self.assertFalse(self.summary()["published"])

    def test_same_run_rerun_failure_invalidates_success_then_success_has_same_counts(self):
        self.resolve(); self.acquire(); result = self.execute()
        telemetry.complete(self.plan, self.run_id, result, self.release(), self.env)
        original = self.summary()["sources"]
        self.resolve(); self.acquire()
        with patch.object(runtime, "invoke", side_effect=runtime.BackfillError("BACKFILL_ADAPTER_FAILED")):
            with self.assertRaises(obs.ObservabilityError): runtime.execute(self.plan, self.env, telemetry_context=self.context)
        self.release(); self.assertEqual("FAILED", self.summary()["status"])
        self.assertEqual([None, None], [row["counts"]["input"] for row in self.summary()["sources"]])
        self.resolve(); self.acquire(); result = self.execute()
        telemetry.complete(self.plan, self.run_id, result, self.release(), self.env)
        self.assertEqual(original, self.summary()["sources"])

    def test_new_airflow_run_same_operation_has_separate_journal_and_same_business_scope(self):
        self.resolve(); before = self.context.copy()
        self.resolve(run_id="manual:observability:2")
        self.assertEqual(before["scope_sha256"], self.context["scope_sha256"])
        self.assertNotEqual(before["context_sha256"], self.context["context_sha256"])
        self.assertNotEqual(obs._root(before, self.env), obs._root(self.context, self.env))

    def test_failed_cleanup_dag_callback_preserves_source_failure(self):
        self.resolve(); self.acquire()
        with patch.object(runtime, "invoke", side_effect=runtime.BackfillError("BACKFILL_ADAPTER_FAILED")):
            with self.assertRaises(obs.ObservabilityError): runtime.execute(self.plan, self.env, telemetry_context=self.context)
        self.release()
        obs.dag_failure_summary({"dag_id": "orc_03_backfill", "run_id": self.run_id}, self.env)
        self.assertEqual("source", self.summary()["failed_phase"])
        self.assertEqual("FAILED", self.summary()["status"])


if __name__ == "__main__": unittest.main()
