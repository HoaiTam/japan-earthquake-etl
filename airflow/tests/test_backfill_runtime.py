"""Small exact-scope control-plane fixtures; no raw earthquake transformation in Python."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import backfill_runtime as runtime
import etl_pipeline_runtime as etl
from source_run_guard import lease, SourceRunBusy

REPO = Path(__file__).parents[2]


def pinned():
    return [
        {"source_system": "USGS", "manifest_uri": "s3://lake/bronze/usgs/run_id=old/manifest.json",
         "sha256": "a" * 64, "manifest_sha256": "b" * 64,
         "window_start_utc": "2023-01-01T00:00:00Z", "window_end_utc": "2023-01-04T00:00:00Z"},
        {"source_system": "JMA_BULLETIN",
         "manifest_uri": "s3://lake/bronze/jma/year=2023/catalog_release=old-release/run_id=old/manifest.json",
         "sha256": "c" * 64, "manifest_sha256": "d" * 64,
         "year": 2023, "segment": "full-year", "catalog_release": "old-release"}]


def base(action="ingest"):
    result = {"operation_id": "test-op-v1", "processing_date": "2026-10-08", "action": action,
              "preview": False}
    if action == "ingest":
        result["usgs"] = {"window_start_utc": "2023-01-01T00:00:00Z",
                          "window_end_utc": "2023-01-08T00:00:00Z", "chunk_days": 3}
        result["jma"] = {"years": [1997], "segments": [{"year": 1997, "segment": "oct-dec"}]}
    else:
        result["bronze_inputs"] = pinned()
    if action == "reprocess":
        outputs = deepcopy(etl.load_mock_fixture()["conf"]["output_scope"])
        result["reprocess"] = {"window_start_utc": "2023-01-01T00:00:00Z",
            "window_end_utc": "2023-01-04T00:00:00Z", "output_scope": outputs,
            "gold_partitions": [{"table": table, "event_year_utc": 2023, "event_month_utc": 1}
                                for table in outputs["gold_tables"]],
            "existing_silver_manifests": [{"manifest_uri": "s3://lake/silver/existing/manifest.json", "sha256": "e" * 64}],
            "baseline_snapshots": [{"table": table, "snapshot_id": "101"} for table in outputs["gold_tables"]]}
    return result


def adapter_receipt(plan, phase, upstream):
    """Contract test double, not evidence of real Iceberg/Spark processing."""
    context = plan["reprocess"]["etl_context"]
    artifacts = deepcopy(etl.load_mock_fixture()["artifacts"][phase])
    def real(value):
        if isinstance(value, str): return value.replace("mock://", "s3://").replace("mock-", "")
        if isinstance(value, list): return [real(item) for item in value]
        if isinstance(value, dict): return {key: real(item) for key, item in value.items()}
        return value
    artifacts = real(artifacts)
    manifests = context["input_scope"]["bronze_manifests"]
    uris = [item["manifest_uri"] for item in manifests]
    if phase == "readiness": artifacts["bronze_manifest_uris"] = uris
    if phase == "bronze":
        for item, pin in zip(artifacts["bronze_inputs"], manifests): item.update(pin)
    if phase == "silver":
        artifacts["input_manifest_uris"] = uris
        artifacts["partitions"] = context["output_scope"]["silver_partitions"]
    if phase == "publish": artifacts.update(published=True, publication_status="Published")
    old = upstream["etl_receipt"] if upstream else None
    receipt = {"contract_version": etl.CONTRACT_VERSION, "run_id": context["run_id"],
               "context_sha256": context["context_sha256"], "mode": "real", "phase": phase,
               "status": "ok", "upstream_sha256": etl._digest(old) if old else None, "artifacts": artifacts}
    return {"etl_receipt": receipt, "scope_receipt": {"contract_version": "orc-03-v1",
            "scope_sha256": plan["scope_sha256"], "operation_id": plan["operation_id"], "phase": phase,
            "baseline_verified": True, "idempotency_verified": True, "outside_scope_unchanged": True}}


class BackfillTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.env = {"JMA_INVENTORY_PATH": str(REPO / "config/jma/hypocenter_archives_v1.csv"),
                    "CONFIG_VERSION": "1", "USGS_MAX_WINDOW_DAYS": "3",
                    "DATA_BUCKET": "lake", "BRONZE_PREFIX": "bronze",
                    "BACKFILL_STAGING_ROOT": str(Path(self.temporary.name) / "backfill"),
                    "JMA_STAGING_ROOT": str(Path(self.temporary.name) / "jma"),
                    "SOURCE_GUARD_ROOT": str(Path(self.temporary.name) / "guard")}

    def resolve(self, conf):
        return runtime.resolve({"run_id": "manual:operator:1", "dag_run": {"conf": conf}}, self.env)

    def test_preview_defaults_and_never_contacts_services_or_creates_persistent_staging(self):
        conf = base(); conf.pop("preview")
        with patch("subprocess.run", side_effect=AssertionError("preview invoked process")):
            plan = self.resolve(conf)
            summary = runtime.preview_summary(plan)
        self.assertEqual("PREVIEW", summary["status"])
        self.assertFalse(summary["verified"]); self.assertFalse(summary["published"])
        self.assertEqual([], list(Path(self.temporary.name).iterdir()))
        with self.assertRaises(runtime.BackfillError): runtime.execute(plan, self.env)

    def test_usgs_contiguous_half_open_chunks_and_jma_selected_native_segment(self):
        plan = self.resolve(base())
        chunks = plan["usgs"]
        self.assertEqual(3, len(chunks))
        self.assertEqual(chunks[0]["window_end_utc"], chunks[1]["window_start_utc"])
        self.assertEqual("2023-01-08T00:00:00Z", chunks[-1]["window_end_utc"])
        self.assertTrue(all(item["is_backfill"] for item in chunks))
        self.assertEqual(["oct-dec"], [item["segment"] for item in plan["jma"]["archives"]])
        self.assertEqual("1997-09-30T15:00:00Z", plan["jma"]["run_context"]["window_start_utc"])
        self.assertEqual("Bronze only; no Silver/Gold write", runtime.preview_summary(plan)["writes"])

    def test_stable_operation_identity_survives_new_airflow_run_and_preview_flag(self):
        conf = base(); before = self.resolve(conf)
        conf["preview"] = True
        after = runtime.resolve({"run_id": "a-different-airflow-id", "dag_run": {"conf": conf}}, self.env)
        self.assertEqual(before["scope_sha256"], after["scope_sha256"])
        self.assertEqual(before["usgs"], after["usgs"])
        self.assertEqual(before["jma"], after["jma"])

    def test_same_operation_cannot_change_scope_or_public_usgs_request_settings(self):
        conf = base(); before = self.resolve(conf); runtime.pin_operation(before, self.env)
        conf["usgs"]["window_end_utc"] = "2023-01-09T00:00:00Z"
        with self.assertRaisesRegex(runtime.BackfillError, "OPERATION_SCOPE_CHANGED"):
            runtime.pin_operation(self.resolve(conf), self.env)
        env = {**self.env, "USGS_MIN_LATITUDE": "30"}
        changed = runtime.resolve({"dag_run": {"conf": base()}}, env)
        with self.assertRaisesRegex(runtime.BackfillError, "OPERATION_SCOPE_CHANGED"):
            runtime.pin_operation(changed, env)

    def test_bad_types_future_unbounded_ranges_unknown_fields_and_url_injection_fail(self):
        mutations = [lambda c: c.update(preview="false"), lambda c: c.update(operation_id="../escape"),
                     lambda c: c["usgs"].update(chunk_days=True), lambda c: c["usgs"].update(chunk_days=4),
                     lambda c: c["usgs"].update(window_end_utc="2200-01-01T00:00:00Z"),
                     lambda c: c["usgs"].update(window_end_utc="2024-01-01T00:00:00Z"),
                     lambda c: c["jma"].update(url="https://evil.invalid"),
                     lambda c: c["jma"].update(segments=[{"year": 2023, "segment": "full-year"}]),
                     lambda c: c.update(command="echo pretend-ready")]
        for mutate in mutations:
            conf = base(); mutate(conf)
            with self.subTest(conf=conf), self.assertRaises((runtime.BackfillError, etl.EtlContractError)):
                self.resolve(conf)
        combined = base()
        combined["usgs"]["window_end_utc"] = "2023-04-07T00:00:00Z"  # 32 requests + 41 archives > 64 pins.
        combined["jma"] = {"years": list(range(1984, 2024))}
        with self.assertRaisesRegex(runtime.BackfillError, "TOO_MANY_TOTAL_INPUTS"):
            self.resolve(combined)

    def test_reuse_exact_historical_release_and_no_downloader_invocation(self):
        plan = self.resolve(base("reuse")); runtime.pin_operation(plan, self.env)
        response = {"contract_version": "orc-03-v1", "scope_sha256": plan["scope_sha256"],
                    "status": "BronzeVerified", "verified": True, "bronze_inputs": pinned()}
        with patch.object(runtime, "invoke", return_value=response) as verifier, patch.object(
                runtime.usgs, "execute_phase", side_effect=AssertionError("download")), patch.object(
                runtime.jma, "execute_archive", side_effect=AssertionError("download")):
            first = runtime.execute(plan, self.env); second = runtime.execute(plan, self.env)
        self.assertEqual(first, second); self.assertEqual(2, verifier.call_count)
        self.assertFalse(first["published"]); self.assertEqual(pinned(), first["bronze_inputs"])

    def test_tampered_pin_missing_hash_wildcard_release_mismatch_duplicate_rejected(self):
        for mutation in ("missing", "wildcard", "release", "duplicate"):
            conf = base("reuse")
            if mutation == "missing": conf["bronze_inputs"][0].pop("manifest_sha256")
            if mutation == "wildcard": conf["bronze_inputs"][0]["manifest_uri"] = "s3://lake/bronze/*/manifest.json"
            if mutation == "release": conf["bronze_inputs"][1]["catalog_release"] = "other-release"
            if mutation == "duplicate": conf["bronze_inputs"].append(conf["bronze_inputs"][0])
            with self.subTest(mutation=mutation), self.assertRaises((runtime.BackfillError, etl.EtlContractError)):
                self.resolve(conf)

    def test_failed_reuse_invalidates_old_success_and_never_invokes_reprocessing(self):
        plan = self.resolve(base("reuse")); runtime.pin_operation(plan, self.env)
        target = runtime.root(plan, self.env) / "run_summary.json"
        runtime.jma._atomic_json(target, {"status": "Published", "published": True})
        with patch.object(runtime, "invoke", side_effect=runtime.BackfillError("BAD_HASH")), patch.object(
                runtime, "execute_reprocess") as downstream:
            with self.assertRaises(runtime.BackfillError): runtime.execute(plan, self.env)
        self.assertEqual("FAILED", json.loads(target.read_text())["status"])
        downstream.assert_not_called()

    def test_reprocess_requires_both_sources_explicit_old_new_scope_and_baselines(self):
        for mutation in ("one_source", "gold", "silver", "baseline", "unknown"):
            conf = base("reprocess")
            if mutation == "one_source": conf["bronze_inputs"].pop()
            if mutation == "gold": conf["reprocess"]["gold_partitions"] = []
            if mutation == "silver": conf["reprocess"]["output_scope"]["silver_partitions"] = []
            if mutation == "baseline": conf["reprocess"]["baseline_snapshots"] = []
            if mutation == "unknown": conf["reprocess"]["wildcard"] = "silver/**"
            with self.subTest(mutation=mutation), self.assertRaises((runtime.BackfillError, etl.EtlContractError)):
                self.resolve(conf)

    def test_real_reprocess_missing_scoped_adapter_fails_before_staging_or_sources(self):
        plan = self.resolve(base("reprocess"))
        with patch.object(runtime, "execute_sources") as source:
            with self.assertRaisesRegex(runtime.BackfillError, "SCOPED_ETL_ADAPTER_NOT_CONFIGURED"):
                runtime.execute(plan, self.env)
        source.assert_not_called(); self.assertEqual([], list(Path(self.temporary.name).iterdir()))

    def test_scoped_receipts_pin_all_phases_and_rerun_identity(self):
        plan = self.resolve(base("reprocess"))
        bronze = {"status": "BronzeVerified", "scope_sha256": plan["scope_sha256"], "bronze_inputs": pinned()}
        def adapter(command, request, target, env):
            return adapter_receipt(plan, request["phase"], request["upstream"])
        with patch.object(runtime, "invoke", side_effect=adapter) as runner:
            first = runtime.execute_reprocess(plan, bronze, self.env)
            second = runtime.execute_reprocess(plan, bronze, self.env)
        self.assertEqual(first, second); self.assertEqual(12, runner.call_count)
        # Receipt is a test double only; no table, snapshot or real publication was created.

    def test_scope_or_snapshot_gate_failure_prevents_publish_and_later_phases(self):
        plan = self.resolve(base("reprocess"))
        bronze = {"status": "BronzeVerified", "scope_sha256": plan["scope_sha256"], "bronze_inputs": pinned()}
        for field in ("outside_scope_unchanged", "idempotency_verified", "baseline_verified", "scope_sha256"):
            called = []
            def adapter(command, request, target, env):
                phase = request["phase"]; called.append(phase)
                result = adapter_receipt(plan, phase, request["upstream"])
                if phase == "silver": result["scope_receipt"][field] = False
                return result
            with self.subTest(field=field), patch.object(runtime, "invoke", side_effect=adapter):
                with self.assertRaisesRegex(runtime.BackfillError, "SCOPED_WRITE_GATE_FAILED"):
                    runtime.execute_reprocess(plan, bronze, self.env)
            self.assertEqual(["readiness", "bronze", "silver"], called)

    def test_trino_blocker_never_calls_publication(self):
        plan = self.resolve(base("reprocess")); called = []
        bronze = {"status": "BronzeVerified", "scope_sha256": plan["scope_sha256"], "bronze_inputs": pinned()}
        def adapter(command, request, target, env):
            phase = request["phase"]; called.append(phase)
            result = adapter_receipt(plan, phase, request["upstream"])
            if phase == "verify": result["etl_receipt"]["artifacts"]["checks"]["scope_respected"] = False
            return result
        with patch.object(runtime, "invoke", side_effect=adapter), self.assertRaises(etl.EtlContractError):
            runtime.execute_reprocess(plan, bronze, self.env)
        self.assertNotIn("publish", called)

    def test_plan_hash_tamper_or_dryrun_cannot_start(self):
        plan = self.resolve(base()); plan["usgs"][0]["window_end_utc"] = "2023-01-05T00:00:00Z"
        with self.assertRaisesRegex(runtime.BackfillError, "PLAN_CHANGED"): runtime.pin_operation(plan, self.env)
        with self.assertRaisesRegex(runtime.BackfillError, "DRY_RUN_FORBIDDEN"):
            runtime.pin_operation(self.resolve(base()), {**self.env, "USGS_INGEST_DRY_RUN": "true"})

    def test_subprocess_is_fixed_argv_bounded_and_duplicate_json_fields_rejected(self):
        target = Path(self.temporary.name) / "request.json"
        def process(argv, **options):
            self.assertEqual(["adapter", "--context-file", str(target)], argv)
            self.assertFalse(options["shell"]); self.assertEqual(subprocess.DEVNULL, options["stderr"])
            options["stdout"].write(b'{"verified":true,"verified":false}')
            return subprocess.CompletedProcess(argv, 0)
        with patch("subprocess.run", side_effect=process), self.assertRaises(etl.EtlContractError):
            runtime.invoke("adapter", {}, target, self.env)
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("secret", 1)):
            with self.assertRaisesRegex(runtime.BackfillError, "BACKFILL_ADAPTER_DID_NOT_COMPLETE"):
                runtime.invoke("adapter", {}, target, self.env)

    def test_cross_dag_contention_and_foreign_cleanup_never_release_owner(self):
        daily = {"dag_id": "orc_02_daily_sources", "run_id": "daily"}
        backfill = {"dag_id": "orc_03_backfill", "run_id": "backfill"}
        lease("acquire", daily, self.env)
        with self.assertRaises(SourceRunBusy): lease("acquire", backfill, self.env)
        self.assertEqual("NOT_OWNER", lease("release", backfill, self.env)["status"])
        lease("assert", daily, self.env); lease("release", daily, self.env)
        lease("acquire", backfill, self.env); lease("assert", backfill, self.env)

    def test_failed_middle_chunk_stops_other_sources_and_success_cannot_leak(self):
        plan = self.resolve(base()); called = []
        def phase(name, context, upstream, env):
            called.append((context["run_id"], name))
            if context == plan["usgs"][1]: raise runtime.BackfillError("HTTP_FAILED")
            if name == "validate": return {"valid": True}
            return {"run_id": context["run_id"], "bronze_status": "BronzeReady", "verified": True,
                    "manifest_uri": "s3://lake/bronze/usgs/one/manifest.json", "manifest_sha256": "a" * 64,
                    "sha256": "b" * 64}
        with patch.object(runtime.usgs, "execute_phase", side_effect=phase), patch.object(
                runtime.jma, "execute_archive") as jma, patch.object(runtime, "verify_pinned") as verify:
            with self.assertRaises(runtime.BackfillError): runtime.execute(plan, self.env)
        self.assertEqual(["fetch", "validate", "upload", "verify", "fetch"], [name for child, name in called])
        jma.assert_not_called(); verify.assert_not_called()
        summary = json.loads((runtime.root(plan, self.env) / "run_summary.json").read_text())
        self.assertEqual("FAILED", summary["status"]); self.assertFalse(summary["published"])

    def test_execution_environment_drift_and_forged_numeric_boolean_fail_closed(self):
        plan = self.resolve(base())
        for delta in ({"DATA_BUCKET": "other-lake"}, {"USGS_MIN_LATITUDE": "30"}, {"CONFIG_VERSION": "2"}):
            with self.subTest(delta=delta), self.assertRaises(runtime.BackfillError):
                runtime.pin_operation(plan, {**self.env, **delta})
        plan = self.resolve(base("reuse"))
        response = {"contract_version": "orc-03-v1", "scope_sha256": plan["scope_sha256"],
                    "status": "BronzeVerified", "verified": 1, "bronze_inputs": pinned()}
        with patch.object(runtime, "invoke", return_value=response), self.assertRaises(runtime.BackfillError):
            runtime.verify_pinned(plan, pinned(), self.env)

    def test_deployment_includes_readback_wrapper_in_docker_context(self):
        wrapper = "compose/airflow/bronze-reuse-runner.sh"
        self.assertIn("!" + wrapper, (REPO / ".dockerignore").read_text().splitlines())
        self.assertIn(wrapper, (REPO / "compose/airflow/Dockerfile").read_text())
        self.assertIn("BronzeReuseVerifier", (REPO / wrapper).read_text())
        self.assertIn("BACKFILL_ETL_RUNNER_COMMAND: ${BACKFILL_ETL_RUNNER_COMMAND:-}", (REPO / "compose.yaml").read_text())

    def test_live_qa_helper_releases_lease_after_failure_without_download_fallback(self):
        import backfill_readback_qa as qa
        with patch.object(qa, "execute", side_effect=runtime.BackfillError("OBJECT_MISSING")) as runner:
            with self.assertRaises(runtime.BackfillError): qa.run(self.env)
        self.assertEqual(1, runner.call_count)
        self.assertEqual("reuse", runner.call_args.args[0]["action"])
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_scoped_write_rejects_one_as_boolean_and_baseline_needs_existing_state(self):
        conf = base("reprocess"); conf["reprocess"]["existing_silver_manifests"] = []
        with self.assertRaisesRegex(runtime.BackfillError, "EXISTING_SILVER_CONTEXT_REQUIRED"):
            self.resolve(conf)
        plan = self.resolve(base("reprocess"))
        bronze = {"status": "BronzeVerified", "scope_sha256": plan["scope_sha256"], "bronze_inputs": pinned()}
        def adapter(command, request, target, env):
            result = adapter_receipt(plan, request["phase"], request["upstream"])
            result["scope_receipt"]["outside_scope_unchanged"] = 1
            return result
        with patch.object(runtime, "invoke", side_effect=adapter), self.assertRaises(runtime.BackfillError):
            runtime.execute_reprocess(plan, bronze, self.env)


if __name__ == "__main__": unittest.main()
