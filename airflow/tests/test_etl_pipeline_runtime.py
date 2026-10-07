"""ORC-01 adapter contract tests: synthetic metadata, no external services."""

from copy import deepcopy
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import etl_pipeline_runtime as runtime


class EtlPipelineRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.fixture = runtime.load_mock_fixture()
        self.context = runtime.resolve_run_context({"run_id": "manual__2026-10-07T00:00:00+00:00",
                                                    "dag_run": {"conf": {"mode": "mock"}}}, {})
        self.receipts = {}
        upstream = None
        for phase in runtime.PHASES:
            upstream = runtime.execute_phase(phase, self.context, upstream, {})
            self.receipts[phase] = upstream

    def previous(self, phase):
        position = runtime.PHASES.index(phase)
        return self.receipts[runtime.PHASES[position - 1]] if position else None

    def invalid(self, phase, change, reason):
        result = deepcopy(self.receipts[phase])
        change(result)
        with self.assertRaisesRegex(runtime.EtlContractError, reason):
            runtime.validate_result(phase, self.context, self.previous(phase), result)

    def real_context(self, environment=None):
        conf = json.loads(json.dumps(self.fixture["conf"]).replace("mock://orc-01/", "s3://test-bucket/"))
        conf["mode"] = "real"
        return runtime.resolve_run_context({"run_id": "real-protocol-unit-test", "dag_run": {"conf": conf}},
                                           environment or {"ETL_PHASE_RUNNER_COMMAND": "/unit-test/adapter"})

    def real_result(self, phase, context, upstream):
        artifacts = json.loads(json.dumps(self.fixture["artifacts"][phase])
                               .replace("mock://orc-01/", "s3://test-bucket/")
                               .replace('"mock-101"', '"101"').replace('"mock-102"', '"102"'))
        if phase == "publish":
            artifacts.update(published=True, publication_status="Published")
        return {"contract_version": runtime.CONTRACT_VERSION, "run_id": context["run_id"],
                "context_sha256": context["context_sha256"], "mode": "real", "phase": phase,
                "status": "ok", "upstream_sha256": runtime._digest(upstream) if upstream else None,
                "artifacts": artifacts}

    def test_fixture_chain_stops_at_mock_complete_never_published(self):
        summary = runtime.publication_summary(self.context, self.receipts["publish"])
        self.assertEqual("MockComplete", summary["publication_status"])
        self.assertIs(False, summary["published"])
        self.assertEqual("[start,end)", self.context["interval_semantics"])
        self.assertEqual("2023-01-03", self.context["processing_date"])
        self.assertIs(True, self.context["is_backfill"])
        self.assertEqual(2, len(summary["snapshots"]))

    def test_mock_default_is_offline_and_has_no_staging_side_effect(self):
        with patch.object(runtime.subprocess, "run") as process, patch.object(runtime, "_write_request") as writer:
            context = runtime.resolve_run_context({"run_id": "default-mock", "dag_run": {"conf": {}}}, {})
            runtime.execute_phase("readiness", context, environment={})
        process.assert_not_called()
        writer.assert_not_called()

    def test_context_deterministic_and_configuration_version_from_environment(self):
        args = {"run_id": "same-run", "dag_run": {"conf": {}}}
        first = runtime.resolve_run_context(args, {"CONFIG_VERSION": "2"})
        self.assertEqual(first, runtime.resolve_run_context(args, {"CONFIG_VERSION": "2"}))
        self.assertNotEqual(first["context_sha256"], runtime.resolve_run_context(args, {"CONFIG_VERSION": "3"})["context_sha256"])

    def test_invalid_windows_flags_mode_and_untrusted_command_are_rejected(self):
        cases = [({"mode": "dry-run"}, "INVALID_EXECUTION_MODE"),
                 ({"window_start_utc": "2023-01-04T00:00:00Z"}, "INVALID_UTC_INTERVAL"),
                 ({"window_end_utc": "2023-01-04T00:00:00"}, "UTC_Z_REQUIRED"),
                 ({"window_end_utc": "2200-01-04T00:00:00Z"}, "INVALID_UTC_INTERVAL"),
                 ({"is_backfill": "false"}, "INVALID_BACKFILL_FLAG"),
                 ({"processing_date": "not-a-date"}, "INVALID_PROCESSING_DATE"),
                 ({"runner_command": "untrusted-command"}, "UNSUPPORTED_RUN_CONFIGURATION"),
                 ({"config_version": "from-conf"}, "UNSUPPORTED_RUN_CONFIGURATION")]
        for conf, reason in cases:
            with self.subTest(conf=conf), self.assertRaisesRegex(runtime.EtlContractError, reason):
                runtime.resolve_run_context({"run_id": "invalid", "dag_run": {"conf": conf}}, {})

    def test_real_mode_needs_explicit_scopes_and_adapter(self):
        with self.assertRaisesRegex(runtime.EtlContractError, "EXPLICIT_REAL_SCOPE_REQUIRED"):
            runtime.resolve_run_context({"run_id": "real", "dag_run": {"conf": {"mode": "real"}}}, {})
        conf = deepcopy(self.fixture["conf"])
        conf["mode"] = "real"
        with self.assertRaisesRegex(runtime.EtlContractError, "INVALID_ARTIFACT_URI"):
            runtime.resolve_run_context({"run_id": "real", "dag_run": {"conf": conf}}, {})
        # Valid real scope, but an executable still must be configured before any task runs.
        with self.assertRaisesRegex(runtime.EtlContractError, "REAL_ADAPTER_NOT_CONFIGURED"):
            self.real_context({"CONFIG_VERSION": "1"})

    def test_invalid_exact_scopes_and_uri_credentials_fail_closed(self):
        for bad_uri in ("mock://orc-01/bronze/*", "mock://orc-01/../manifest.json",
                        "mock://user:password@orc-01/a", "mock://orc-01/a?token=secret", "mock://orc-01/a#secret",
                        "s3://test-bucket/a", "mock://orc-01/a%2Fb"):
            conf = deepcopy(self.fixture["conf"])
            conf["input_scope"]["bronze_manifests"][0]["manifest_uri"] = bad_uri
            with self.subTest(uri=bad_uri), self.assertRaisesRegex(runtime.EtlContractError, "INVALID_ARTIFACT_URI"):
                runtime.resolve_run_context({"run_id": "bad-uri", "dag_run": {"conf": conf}}, {})
        for mutate in (lambda conf: conf["input_scope"]["bronze_manifests"].pop(),
                       lambda conf: conf["output_scope"]["gold_tables"].pop(),
                       lambda conf: conf["output_scope"]["silver_partitions"][0].update(event_month_utc=13)):
            conf = deepcopy(self.fixture["conf"])
            mutate(conf)
            with self.assertRaises(runtime.EtlContractError):
                runtime.resolve_run_context({"run_id": "bad-scope", "dag_run": {"conf": conf}}, {})

    def test_context_mutation_or_stale_run_result_is_rejected(self):
        for field, value in (("run_id", "other-run"), ("config_version", "2"), ("window_end_utc", "2023-01-05T00:00:00Z")):
            context = deepcopy(self.context)
            context[field] = value
            with self.subTest(field=field), self.assertRaisesRegex(runtime.EtlContractError, "CONTEXT_CHANGED"):
                runtime.execute_phase("readiness", context, environment={})
        self.invalid("bronze", lambda result: result.update(run_id="other-run"), "RESULT_CONTEXT_MISMATCH")
        self.invalid("bronze", lambda result: result.update(mode="real"), "RESULT_CONTEXT_MISMATCH")
        self.invalid("bronze", lambda result: result.update(context_sha256="0" * 64), "RESULT_CONTEXT_MISMATCH")

    def test_missing_wrong_or_failed_upstream_cannot_invoke_adapter(self):
        with patch.object(runtime, "mock_result") as adapter:
            for upstream in (None, self.receipts["readiness"], {**self.receipts["bronze"], "status": "failed"}):
                with self.subTest(upstream=upstream), self.assertRaises(runtime.EtlContractError):
                    runtime.execute_phase("silver", self.context, upstream, {})
        adapter.assert_not_called()
        self.invalid("silver", lambda result: result.update(upstream_sha256="0" * 64), "RESULT_UPSTREAM_MISMATCH")

    def test_failure_in_any_phase_stops_all_later_phases(self):
        for failed in runtime.PHASES:
            executed = []
            original = runtime.mock_result
            def adapter(phase, context, upstream):
                executed.append(phase)
                result = original(phase, context, upstream)
                if phase == failed:
                    result["status"] = "failed"
                return result
            with self.subTest(phase=failed), patch.object(runtime, "mock_result", side_effect=adapter):
                with self.assertRaisesRegex(runtime.EtlContractError, "PHASE_FAILED"):
                    upstream = None
                    for phase in runtime.PHASES:
                        upstream = runtime.execute_phase(phase, self.context, upstream, {})
            self.assertEqual(list(runtime.PHASES[:runtime.PHASES.index(failed) + 1]), executed)

    def test_readiness_and_bronze_are_pinned_and_all_five_checks_required(self):
        self.invalid("readiness", lambda result: result["artifacts"].update(sources_ready=["USGS"]), "SOURCE_NOT_READY")
        self.invalid("bronze", lambda result: result["artifacts"]["bronze_inputs"][0].update(manifest_uri="mock://orc-01/other.json"), "BRONZE_INPUT_CHANGED")
        self.invalid("bronze", lambda result: result["artifacts"]["bronze_inputs"][0].update(bronze_status="Rejected"), "BRONZE_NOT_READY")
        self.invalid("bronze", lambda result: result["artifacts"]["bronze_inputs"][0].update(sha256="invalid"), "BRONZE_NOT_READY")
        for check in runtime.BRONZE_CHECKS:
            with self.subTest(check=check):
                self.invalid("bronze", lambda result: result["artifacts"]["bronze_inputs"][0]["validation"].update({check: False}), "QUALITY_GATE_FAILED")

    def test_silver_requires_four_datasets_readback_quality_and_exact_scope(self):
        self.invalid("silver", lambda result: result["artifacts"]["datasets"].pop(), "INCOMPLETE_SILVER_BUNDLE")
        self.invalid("silver", lambda result: result["artifacts"]["datasets"][0].update(readback_verified=False), "SILVER_NOT_READY")
        self.invalid("silver", lambda result: result["artifacts"].update(quality_passed=False), "SILVER_NOT_READY")
        self.invalid("silver", lambda result: result["artifacts"]["partitions"].pop(), "SILVER_SCOPE_CHANGED")
        self.invalid("silver", lambda result: result["artifacts"].update(input_manifest_uris=[]), "SILVER_SCOPE_CHANGED")

    def test_zero_record_estimate_is_valid_not_a_readiness_failure(self):
        result = deepcopy(self.receipts["bronze"])
        result["artifacts"]["bronze_inputs"][0]["record_count_estimate"] = 0
        result = runtime.validate_result("bronze", self.context, self.receipts["readiness"], result)
        runtime.execute_phase("silver", self.context, result, {})
        self.invalid("bronze", lambda receipt: receipt["artifacts"]["bronze_inputs"][0].update(record_count_estimate=-1), "BRONZE_NOT_READY")

    def test_revision_changes_must_resolve_a_new_pinned_context_not_overwrite_bronze(self):
        # Bronze ingestion, not the ETL run, owns immutable revision/release identity.
        result = deepcopy(self.receipts["bronze"])
        result["artifacts"]["bronze_inputs"][0].update(sha256="c" * 64, ingest_run_id="mock-later-ingest")
        with self.assertRaisesRegex(runtime.EtlContractError, "BRONZE_INPUT_CHANGED"):
            runtime.validate_result("bronze", self.context, self.receipts["readiness"], result)
        with self.assertRaisesRegex(runtime.EtlContractError, "BRONZE_INPUT_CHANGED"):
            runtime.validate_result("silver", self.context, result, self.receipts["silver"])

    def test_gold_requires_complete_committed_bundle_and_exact_silver_inputs(self):
        self.invalid("gold", lambda result: result["artifacts"]["snapshots"].pop(), "INCOMPLETE_GOLD_BUNDLE")
        self.invalid("gold", lambda result: result["artifacts"]["snapshots"][0].update(committed=False), "UNCOMMITTED_GOLD_SNAPSHOT")
        self.invalid("gold", lambda result: result["artifacts"]["snapshots"][0].update(snapshot_id="latest"), "UNCOMMITTED_GOLD_SNAPSHOT")
        self.invalid("gold", lambda result: result["artifacts"].update(scope_verified=False), "GOLD_SCOPE_CHANGED")
        self.invalid("gold", lambda result: result["artifacts"].update(input_manifest_uris=["mock://orc-01/latest.json"]), "GOLD_INPUT_CHANGED")

    def test_verify_requires_same_snapshots_trino_and_every_blocker_passed(self):
        self.invalid("verify", lambda result: result["artifacts"]["snapshots"][0].update(snapshot_id="mock-999"), "VERIFIED_SNAPSHOT_CHANGED")
        self.invalid("verify", lambda result: result["artifacts"].update(engine="Spark"), "TRINO_VERIFICATION_REQUIRED")
        self.invalid("verify", lambda result: result["artifacts"]["checks"].pop("readable"), "INVALID_METADATA_FIELDS")
        for check in runtime.VERIFY_CHECKS:
            with self.subTest(check=check):
                self.invalid("verify", lambda result: result["artifacts"]["checks"].update({check: False}), "QUALITY_GATE_FAILED")
        failed = deepcopy(self.receipts["verify"])
        failed["artifacts"]["checks"]["canonical_unique"] = False
        with patch.object(runtime, "mock_result") as adapter, self.assertRaisesRegex(runtime.EtlContractError, "QUALITY_GATE_FAILED"):
            runtime.execute_phase("publish", self.context, failed, {})
        adapter.assert_not_called()

    def test_publish_cannot_switch_snapshot_report_or_execution_mode(self):
        self.invalid("publish", lambda result: result["artifacts"].update(published=True, publication_status="Published"), "PUBLICATION_MODE_MISMATCH")
        self.invalid("publish", lambda result: result["artifacts"]["snapshots"][0].update(snapshot_id="mock-999"), "PUBLICATION_INPUT_CHANGED")
        self.invalid("publish", lambda result: result["artifacts"].update(verification_report_uri="mock://orc-01/other.json"), "PUBLICATION_INPUT_CHANGED")
        with self.assertRaisesRegex(runtime.EtlContractError, "MOCK_IN_REAL_MODE"):
            runtime.mock_result("readiness", self.real_context(), None)

    def test_payload_secret_fields_and_non_finite_metadata_are_rejected(self):
        for phase in runtime.PHASES:
            with self.subTest(phase=phase):
                self.invalid(phase, lambda result: result.update(raw_payload="not-allowed"), "INVALID_METADATA_FIELDS")
                self.invalid(phase, lambda result: result["artifacts"].update(secret="not-allowed"), "INVALID_METADATA_FIELDS")
        self.invalid("readiness", lambda result: result.update(status=float("nan")), "PHASE_FAILED")
        self.invalid("readiness", lambda result: result.update(upstream_sha256="x" * 65536), "METADATA_TOO_LARGE")

    def test_real_protocol_chain_uses_scoped_context_files_not_business_transforms(self):
        context = self.real_context()
        with tempfile.TemporaryDirectory() as directory:
            environment = {"ETL_PHASE_RUNNER_COMMAND": "/unit-test/adapter --profile scoped",
                           "ETL_STAGING_ROOT": directory}
            upstream = None
            def process(argv, **kwargs):
                request = json.loads(Path(argv[-1]).read_text())
                self.assertEqual(["--phase", request["phase"], "--context-file"], argv[-4:-1])
                self.assertEqual(context, request["run_context"])
                self.assertEqual(upstream, request["upstream"])
                self.assertEqual(runtime._digest(upstream) if upstream else None, request["upstream_sha256"])
                self.assertIs(False, kwargs["shell"])
                self.assertEqual(subprocess.DEVNULL, kwargs["stderr"])
                kwargs["stdout"].write(json.dumps(self.real_result(request["phase"], context, upstream)).encode())
                return subprocess.CompletedProcess(argv, 0)
            with patch.object(runtime.subprocess, "run", side_effect=process) as adapter:
                for phase in runtime.PHASES:
                    upstream = runtime.execute_phase(phase, context, upstream, environment)
            self.assertEqual(6, adapter.call_count)
            self.assertEqual(6, len(list(Path(directory).rglob("*-input.json"))))
            self.assertTrue(runtime.publication_summary(context, upstream)["published"])
            # This is a protocol unit test; the subprocess is mocked, no Iceberg publication exists.
            # Exercise real OS stdout spooling with a metadata emitter, still no source/store calls.
            synthetic = self.real_result("readiness", context, None)
            emitter = shlex.join([sys.executable, "-c", "import sys; sys.stdout.write(" + repr(json.dumps(synthetic)) + ")"])
            receipt = runtime.execute_phase("readiness", context,
                                            environment={**environment, "ETL_PHASE_RUNNER_COMMAND": emitter})
            self.assertEqual(synthetic, receipt)

    def test_real_adapter_errors_have_safe_reasons_and_never_echo_raw_logs(self):
        context = self.real_context()
        with tempfile.TemporaryDirectory() as directory:
            environment = {"ETL_PHASE_RUNNER_COMMAND": "/unit-test/adapter", "ETL_STAGING_ROOT": directory}
            def output(value, returncode=0):
                def process(argv, **kwargs):
                    kwargs["stdout"].write(value)
                    return subprocess.CompletedProcess(argv, returncode, stderr="password=secret")
                return process
            for effect, reason in ((output(b"password=secret", 1), "ADAPTER_PROCESS_FAILED"),
                                   (output(b"password=secret"), "INVALID_ADAPTER_JSON"),
                                   (output(b'{"phase":"readiness","phase":"publish"}'), "DUPLICATE_JSON_FIELD"),
                                   (output(b"x" * 65537), "METADATA_TOO_LARGE"),
                                   (subprocess.TimeoutExpired("adapter", 1), "ADAPTER_DID_NOT_COMPLETE")):
                with self.subTest(reason=reason), patch.object(runtime.subprocess, "run", side_effect=effect):
                    with self.assertRaisesRegex(runtime.EtlContractError, reason) as error:
                        runtime.execute_phase("readiness", context, environment=environment)
                    self.assertNotIn("secret", str(error.exception))
            for timeout in ("not-int", "0", "86401"):
                with self.subTest(timeout=timeout), self.assertRaisesRegex(runtime.EtlContractError, "INVALID_ADAPTER_CONFIGURATION"):
                    runtime.execute_phase("readiness", context, environment={**environment, "ETL_PHASE_RUNNER_TIMEOUT_SECONDS": timeout})

    def test_rerun_keeps_fixture_immutable_and_ingest_lineage_separate(self):
        before = runtime.FIXTURE_PATH.read_bytes()
        first = runtime.execute_phase("bronze", self.context, self.receipts["readiness"], {})
        second = runtime.execute_phase("bronze", self.context, self.receipts["readiness"], {})
        self.assertEqual(first, second)
        self.assertEqual(before, runtime.FIXTURE_PATH.read_bytes())
        self.assertNotEqual(self.context["run_id"], first["artifacts"]["bronze_inputs"][0]["ingest_run_id"])
        first["artifacts"]["bronze_inputs"].clear()
        self.assertEqual(2, len(second["artifacts"]["bronze_inputs"]))


if __name__ == "__main__":
    unittest.main()
