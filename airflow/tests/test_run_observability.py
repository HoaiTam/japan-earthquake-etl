"""ORC-04 telemetry/adapter regressions. Only bounded synthetic metadata."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import etl_pipeline_runtime as etl
import run_observability as obs


class RunObservabilityTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = {"RUN_SUMMARY_ROOT": self.temp.name, "CONFIG_VERSION": "test-v1"}
        self.airflow_context = {"run_id": "orc-04-test", "dag_run": {"conf": {"mode": "mock"}}}
        self.context = etl.resolve_observed_context(self.airflow_context, self.env)

    def chain(self, finish=True):
        upstream = None
        for phase in etl.PHASES:
            upstream = etl.execute_observed_phase(phase, self.context, upstream, self.env)
        if finish:
            etl.observed_publication_summary(self.context, upstream, self.env)
        return upstream

    def summary(self):
        return obs.read_summary(self.context, self.env)

    def silver(self):
        ready = etl.execute_phase("readiness", self.context, environment={})
        bronze = etl.execute_phase("bronze", self.context, ready, {})
        silver = etl.execute_phase("silver", self.context, bronze, {})
        return bronze, silver

    def test_success_persists_exact_inputs_snapshots_counts_and_mock_boundary(self):
        with self.assertLogs(obs.LOGGER, level="INFO") as logs:
            receipt = self.chain()
        summary = self.summary()
        self.assertEqual("MockComplete", summary["status"])
        self.assertFalse(summary["published"])
        self.assertEqual("synthetic", summary["evidence_kind"])
        self.assertEqual(receipt["artifacts"]["snapshots"], summary["snapshots"])
        self.assertEqual(4, summary["gold_counts"]["current"])
        self.assertIsNone(summary["gold_counts"]["published"])
        self.assertEqual([], summary["not_executed_phases"])
        for source in summary["sources"]:
            self.assertEqual(2, source["counts"]["input"])
            self.assertEqual("passed", source["reconciliation"])
            self.assertIsNone(source["counts"]["fetched"])
            self.assertEqual(1, len(source["bronze_inputs"]))
        self.assertTrue(any("pipeline_run_summary" in line for line in logs.output))
        root = obs._root(obs.identity(self.context), self.env)
        self.assertEqual(16, len(list((root / "events").iterdir())))
        self.assertTrue(all(path.stat().st_mode & 0o777 == 0o600 for path in (root / "events").iterdir()))

    def test_each_phase_failure_has_reason_and_stops_without_published(self):
        for failed in etl.PHASES:
            with self.subTest(phase=failed):
                etl.resolve_observed_context(self.airflow_context, self.env, attempt=2)
                original = etl.mock_result
                called = []
                def adapter(phase, context, upstream):
                    called.append(phase)
                    receipt = original(phase, context, upstream)
                    if phase == failed:
                        receipt["status"] = "failed"
                    return receipt
                with patch.object(etl, "mock_result", side_effect=adapter), self.assertRaisesRegex(
                        obs.ObservabilityError, "PHASE_FAILED"):
                    self.chain()
                summary = self.summary()
                self.assertEqual("FAILED", summary["status"])
                self.assertEqual(failed, summary["failed_phase"])
                self.assertFalse(summary["published"])
                self.assertEqual("PHASE_FAILED", summary["reason"])
                self.assertEqual(list(etl.PHASES[:etl.PHASES.index(failed) + 1]), called)

    def test_empty_counts_are_valid_not_missing(self):
        fixture = deepcopy(etl.load_mock_fixture())
        for item in fixture["artifacts"]["bronze"]["bronze_inputs"]:
            item["record_count_estimate"] = 0
        for row in fixture["observability"]["silver"]["sources"]:
            row["counts"] = {key: 0 for key in obs.COUNT_FIELDS}
        for phase in ("gold", "verify", "publish"):
            fixture["observability"][phase]["gold"] = {"current": 0, "bridge": 0}
        with patch.object(etl, "load_mock_fixture", return_value=fixture):
            self.chain()
        summary = self.summary()
        self.assertEqual(0, summary["gold_counts"]["current"])
        self.assertEqual("passed", summary["gold_counts"]["reconciliation"])

    def test_legacy_receipts_do_not_fabricate_downstream_or_fetched_counts(self):
        fixture = deepcopy(etl.load_mock_fixture())
        fixture.pop("observability")
        with patch.object(etl, "load_mock_fixture", return_value=fixture):
            self.chain()
        summary = self.summary()
        self.assertIsNone(summary["gold_counts"]["current"])
        self.assertEqual("not_reported", summary["gold_counts"]["reconciliation"])
        for row in summary["sources"]:
            self.assertEqual(2, row["counts"]["input"])
            self.assertIsNone(row["counts"]["parsed"])
            self.assertIsNone(row["counts"]["fetched"])

    def test_exact_duplicate_superseded_linked_membership_and_event_grains(self):
        bronze, silver = self.silver()
        for item in bronze["artifacts"]["bronze_inputs"]:
            item["record_count_estimate"] = 10
        report = silver["observability"]
        for row in report["sources"]:
            row["counts"].update(input=10, parsed=8, parse_error=1, ignored=1,
                                 valid=6, rejected=2, duplicate=1, superseded=2, current=3,
                                 linked=2, unlinked=1)
            row["reject_reasons"] = {"INVALID_COORDINATES": 2}
        obs.validate_counts(report, "silver", self.context, bronze)
        gold = {"version": obs.VERSION, "sources": [], "gold": {"current": 4, "bridge": 6}}
        obs.validate_counts(gold, "gold", self.context, {"observability": report})
        # Six observation memberships => four canonical events, not six link pairs.
        self.assertNotEqual(gold["gold"]["current"], gold["gold"]["bridge"])

    def test_every_source_equation_is_blocking(self):
        bronze, silver = self.silver()
        for field in ("input", "parsed", "parse_error", "ignored", "valid", "rejected",
                      "duplicate", "superseded", "current", "linked", "unlinked"):
            report = deepcopy(silver["observability"])
            report["sources"][0]["counts"][field] += 1
            with self.subTest(field=field), self.assertRaisesRegex(obs.ObservabilityError, "COUNT_RECONCILIATION_FAILED"):
                obs.validate_counts(report, "silver", self.context, bronze)

    def test_counts_reject_bool_negative_float_nonfinite_huge_missing_extra(self):
        _, silver = self.silver()
        for value in (True, -1, 1.0, float("nan"), 9223372036854775808, None):
            report = deepcopy(silver["observability"])
            report["sources"][0]["counts"]["input"] = value
            with self.subTest(value=value), self.assertRaises(obs.ObservabilityError):
                obs.validate_counts(report, "silver", self.context)
        for mutation in (lambda row: row["counts"].pop("current"),
                         lambda row: row["counts"].update(password="DO_NOT_LOG"),
                         lambda row: row.update(payload="DO_NOT_LOG")):
            report = deepcopy(silver["observability"])
            mutation(report["sources"][0])
            with self.assertRaises(obs.ObservabilityError):
                obs.validate_counts(report, "silver", self.context)

    def test_source_and_release_shapes_reject_invalid_duplicate_or_missing(self):
        _, silver = self.silver()
        for mutate in (lambda report: report["sources"].pop(),
                       lambda report: report["sources"].append(report["sources"][0]),
                       lambda report: report["sources"][0].update(source_system="UNKNOWN"),
                       lambda report: report["sources"][0].update(catalog_releases=[{}]),
                       lambda report: report["sources"][0].update(catalog_releases=["v1", "v1"]),
                       lambda report: report.update(version="v999")):
            report = deepcopy(silver["observability"])
            mutate(report)
            with self.assertRaises(obs.ObservabilityError):
                obs.validate_counts(report, "silver", self.context)

    def test_reject_primary_reason_counts_do_not_double_count_multi_reason_records(self):
        _, silver = self.silver()
        report = silver["observability"]
        report["sources"][0]["reject_reasons"] = {"INVALID_COORDINATES": 1}
        with self.assertRaisesRegex(obs.ObservabilityError, "REJECT_REASON_COUNT_MISMATCH"):
            obs.validate_counts(report, "silver", self.context)

    def test_bronze_silver_and_silver_gold_reconcile_exact_scope(self):
        bronze, silver = self.silver()
        bronze["artifacts"]["bronze_inputs"][0]["record_count_estimate"] = 999
        with self.assertRaisesRegex(obs.ObservabilityError, "BRONZE_SILVER_COUNT_MISMATCH"):
            obs.validate_counts(silver["observability"], "silver", self.context, bronze)
        report = {"version": obs.VERSION, "sources": [], "gold": {"current": 3, "bridge": 3}}
        with self.assertRaisesRegex(obs.ObservabilityError, "SILVER_GOLD_COUNT_MISMATCH"):
            obs.validate_counts(report, "gold", self.context, silver)

    def test_verification_and_publication_counts_cannot_drift(self):
        gold = {"version": obs.VERSION, "sources": [], "gold": {"current": 4, "bridge": 4}}
        for phase in ("verify", "publish"):
            with self.subTest(phase=phase), self.assertRaisesRegex(obs.ObservabilityError, "VERIFIED_COUNT_CHANGED"):
                obs.validate_counts({**gold, "gold": {"current": 3, "bridge": 4}}, phase,
                                    self.context, {"observability": gold})
        with self.assertRaisesRegex(obs.ObservabilityError, "MISSING_UPSTREAM_COUNTS"):
            obs.validate_counts(gold, "gold", self.context, {})

    def test_rerun_retains_history_but_not_stale_success_or_additive_counts(self):
        self.chain()
        history_root = obs._root(obs.identity(self.context), self.env) / "events"
        initial_events = len(list(history_root.iterdir()))
        etl.resolve_observed_context(self.airflow_context, self.env, attempt=2)
        upstream = etl.execute_observed_phase("readiness", self.context, environment=self.env, attempt=2)
        etl.execute_observed_phase("bronze", self.context, upstream, self.env, attempt=2)
        summary = self.summary()
        self.assertEqual("RUNNING", summary["status"])
        self.assertFalse(summary["published"])
        self.assertEqual([], summary["snapshots"])
        self.assertIsNone(summary["gold_counts"]["current"])
        self.assertEqual(2, summary["sources"][0]["counts"]["input"])
        self.assertGreater(len(list(history_root.iterdir())), initial_events)
        self.chain()
        self.assertEqual(4, self.summary()["gold_counts"]["current"])

    def test_failed_then_successful_retry_not_marked_failed_or_double_counted(self):
        ready = etl.execute_observed_phase("readiness", self.context, environment=self.env)
        with patch.object(etl, "execute_phase", side_effect=etl.EtlContractError("ADAPTER_DID_NOT_COMPLETE")):
            with self.assertRaises(obs.ObservabilityError):
                etl.execute_observed_phase("bronze", self.context, ready, self.env)
        etl.execute_observed_phase("bronze", self.context, ready, self.env, attempt=2)
        summary = self.summary()
        self.assertEqual("RUNNING", summary["status"])
        self.assertIsNone(summary["reason"])
        self.assertEqual(2, next(row for row in summary["sources"] if row["source_system"] == "USGS")["counts"]["input"])

    def test_unknown_external_exception_token_and_payload_never_in_log_or_files(self):
        secret = "SECRET_SENTINEL_04"
        def fail():
            raise RuntimeError("https://user:" + secret + "@host/?token=" + secret + " payload=" + "x" * 100000)
        with self.assertLogs(obs.LOGGER, level="INFO") as logs, self.assertRaisesRegex(
                obs.ObservabilityError, "PIPELINE_OPERATION_FAILED") as caught:
            obs.observe(self.context, "readiness", fail, lambda result: result, self.env)
        self.assertNotIn(secret, str(caught.exception))
        self.assertIsNone(caught.exception.__cause__)
        self.assertNotIn(secret, "\n".join(logs.output))
        for path in Path(self.temp.name).rglob("*.json"):
            self.assertNotIn(secret, path.read_text())
            self.assertLessEqual(path.stat().st_size, obs.MAX_BYTES)

    def test_adapter_secret_extra_field_rejected_before_projection(self):
        ready = etl.execute_observed_phase("readiness", self.context, environment=self.env)
        receipt = etl.mock_result("bronze", self.context, ready)
        receipt["artifacts"]["password"] = "SECRET_SENTINEL_04"
        with patch.object(etl, "mock_result", return_value=receipt), self.assertRaises(obs.ObservabilityError):
            etl.execute_observed_phase("bronze", self.context, ready, self.env)
        for path in Path(self.temp.name).rglob("*.json"):
            self.assertNotIn("SECRET_SENTINEL_04", path.read_text())

    def test_credential_uri_rejected_before_log(self):
        for value in ("s3://user:SECRET@bucket/file", "s3://bucket/file?token=SECRET",
                      "s3://bucket/../file", "s3://bucket/a%2fb", "s3://bucket/*"):
            with self.subTest(value=value), self.assertRaisesRegex(obs.ObservabilityError, "INVALID_TELEMETRY_URI"):
                obs.uri(value, "real")

    def test_invalid_resolve_is_persisted_without_raw_conf(self):
        ctx = {"run_id": "orc-04-bad-conf", "dag_run": {"conf": {"token": "SECRET_SENTINEL_04"}}}
        with self.assertRaises(obs.ObservabilityError):
            etl.resolve_observed_context(ctx, self.env)
        summary = obs.read_summary(obs.unresolved_context(ctx), self.env)
        self.assertEqual("FAILED", summary["status"])
        self.assertEqual("resolve", summary["failed_phase"])
        self.assertNotIn("SECRET_SENTINEL_04", json.dumps(summary))

    def test_duration_uses_monotonic_and_utc_timestamps(self):
        with patch.object(obs.time, "monotonic", side_effect=[10.0, 12.5]):
            etl.execute_observed_phase("readiness", self.context, environment=self.env)
        event = self.summary()["phases"][-1]
        self.assertEqual(2.5, event["duration_seconds"])
        self.assertTrue(event["timestamp_utc"].endswith("Z"))

    def test_full_ids_are_hashed_and_no_path_traversal_or_collision(self):
        one = {**obs.identity(self.context), "run_id": "run/a"}
        two = {**one, "run_id": "run@a"}
        self.assertNotEqual(obs._root(one, self.env), obs._root(two, self.env))
        self.assertEqual(64, len(obs._root(one, self.env).name))

    def test_stale_attempt_cannot_overwrite_newer_phase(self):
        run = obs.identity(self.context)
        first, _ = obs._record(run, "readiness", 1, "RUNNING", None, 0, {}, self.env)
        second, _ = obs._record(run, "readiness", 2, "RUNNING", None, 0, {}, self.env)
        with self.assertRaisesRegex(obs.ObservabilityError, "STALE_TELEMETRY_ATTEMPT"):
            obs._record(run, "readiness", 1, "SUCCEEDED", None, 1, {}, self.env, first)
        obs._record(run, "readiness", 2, "SUCCEEDED", None, 1, {}, self.env, second)

    def test_context_change_without_new_resolve_fails_before_operation(self):
        etl.execute_observed_phase("readiness", self.context, environment=self.env)
        changed = deepcopy(self.context)
        changed["config_version"] = "other-version"
        operation = unittest.mock.Mock()
        with self.assertRaisesRegex(obs.ObservabilityError, "RUN_SUMMARY_CONTEXT_CHANGED"):
            obs.observe(changed, "bronze", operation, lambda result: {}, self.env)
        operation.assert_not_called()

    def test_staging_failure_is_safe_and_cannot_report_success(self):
        with patch.object(obs, "_atomic", side_effect=OSError("SECRET_SENTINEL_04")), self.assertRaisesRegex(
                obs.ObservabilityError, "RUN_SUMMARY_WRITE_FAILED"):
            etl.execute_observed_phase("readiness", self.context, environment=self.env)

    def test_stale_summary_after_interrupted_atomic_write_is_rejected(self):
        self.chain()
        root = obs._root(obs.identity(self.context), self.env)
        state = json.loads((root / "state.json").read_text())
        state["sequence"] += 1
        # Synthetic corruption of our temporary fixture, not a production state edit.
        obs._atomic(root / "state.json", state)
        with self.assertRaisesRegex(obs.ObservabilityError, "STALE_RUN_SUMMARY"):
            self.summary()

    def test_final_gate_requires_persisted_phase_evidence_not_only_publish_receipt(self):
        upstream = None
        for phase in etl.PHASES:
            upstream = etl.execute_phase(phase, self.context, upstream, {})
        with self.assertRaises(obs.ObservabilityError):
            etl.observed_publication_summary(self.context, upstream, self.env)

    def test_final_gate_pins_entire_publish_receipt_including_counts(self):
        receipt = self.chain(finish=False)
        forged = deepcopy(receipt)
        forged["observability"]["gold"]["current"] = 3
        with self.assertRaisesRegex(obs.ObservabilityError, "PUBLICATION_INPUT_CHANGED"):
            etl.observed_publication_summary(self.context, forged, self.env)
        self.assertEqual("FAILED", self.summary()["status"])
        self.assertFalse(self.summary()["published"])

    def test_dag_failure_callback_marks_failed_and_ignores_exception_secret(self):
        self.chain()
        obs.dag_failure_summary({"dag_id": "orc_01_etl_pipeline", "run_id": self.context["run_id"],
                                 "exception": RuntimeError("SECRET_SENTINEL_04")}, self.env)
        summary = self.summary()
        self.assertEqual("FAILED", summary["status"])
        self.assertFalse(summary["published"])
        self.assertNotIn("SECRET_SENTINEL_04", json.dumps(summary))

    def test_projection_does_not_carry_unrecognized_source_receipt_fields(self):
        item = {"bronze_status": "BronzeReady", "verified": True, "run_id": "source-run",
                "manifest_uri": "s3://bucket/manifest.json", "raw_object_uri": "s3://bucket/raw.geojson",
                "sha256": "a" * 64, "manifest_sha256": "b" * 64, "record_count_estimate": 0,
                "password": "SECRET_SENTINEL_04", "payload": "x" * 1000000}
        projected = obs.source_details("USGS", [item])
        self.assertNotIn("SECRET_SENTINEL_04", json.dumps(projected))
        self.assertEqual(0, projected["bronze_inputs"][0]["record_count_estimate"])
        self.assertNotIn("ingest_run_id", projected["bronze_inputs"][0])

    def test_persistence_boundary_rejects_generic_secret_details(self):
        with self.assertRaises(obs.ObservabilityError):
            obs.observe(self.context, "readiness", lambda: None,
                        lambda result: {"password": "SECRET_SENTINEL_04"}, self.env)
        for path in Path(self.temp.name).rglob("*.json"):
            self.assertNotIn("SECRET_SENTINEL_04", path.read_text())

    def test_context_strings_cannot_smuggle_payload_into_summary(self):
        context = {**self.context, "window_end_utc": "SECRET_SENTINEL_04"}
        with self.assertRaises(obs.ObservabilityError):
            obs.observe(context, "readiness", lambda: None, lambda result: {}, self.env)
        for path in Path(self.temp.name).rglob("*.json"):
            self.assertNotIn("SECRET_SENTINEL_04", path.read_text())

    def test_mode_completion_boundary_cannot_promote_mock_or_replay(self):
        for mode in ("mock", "replay"):
            run = {**obs.identity(self.context), "mode": mode}
            with self.subTest(mode=mode), self.assertRaises(obs.ObservabilityError):
                obs.validate_details({"completion_status": "Published"}, "complete", run)

    def test_source_ready_summary_has_release_counts_but_no_silver_gold_inference(self):
        run = {**self.context, "dag_id": "orc_02_daily_sources", "mode": "real", "run_id": "source-only-test"}
        base = {"bronze_status": "BronzeReady", "verified": True, "run_id": "source-run",
                "sha256": "a" * 64, "manifest_sha256": "b" * 64, "record_count_estimate": 2,
                "manifest_uri": "s3://bucket/usgs/manifest.json", "raw_object_uri": "s3://bucket/usgs/raw.geojson"}
        jma = {**base, "record_count_estimate": 100, "year": 2023, "segment": "full-year",
               "catalog_release": "jma-release-v1", "manifest_uri": "s3://bucket/jma/manifest.json",
               "raw_object_uri": "s3://bucket/jma/archive.zip"}
        obs.observe(run, "readiness", lambda: jma,
                    lambda result: obs.source_details("JMA_BULLETIN", [result]), self.env)
        obs.observe(run, "bronze", lambda: base,
                    lambda result: obs.source_details("USGS", [result], run), self.env)
        obs.observe(run, "complete", lambda: None,
                    lambda result: {"completion_status": "SourcesReady"}, self.env)
        summary = obs.read_summary(run, self.env)
        self.assertEqual("SourcesReady", summary["status"])
        self.assertFalse(summary["published"])
        self.assertIsNone(summary["gold_counts"]["current"])
        self.assertEqual(100, summary["sources"][0]["counts"]["input"])
        self.assertEqual(["jma-release-v1"], summary["sources"][0]["catalog_releases"])
        self.assertEqual(2, summary["sources"][1]["counts"]["input"])

    def test_cli_smoke_inspect_and_missing_run_read_only(self):
        import run_summary_cli as cli
        report = cli.smoke(self.temp.name)
        self.assertFalse(report["published"])
        value = cli.inspect_summary("orc_01_etl_pipeline", report["run_id"], self.temp.name)
        self.assertEqual("MockComplete", value["status"])
        missing = obs._root({"dag_id": "orc_01_etl_pipeline", "run_id": "not-exist"}, self.env)
        with self.assertRaises(obs.ObservabilityError):
            cli.inspect_summary("orc_01_etl_pipeline", "not-exist", self.temp.name)
        self.assertFalse(missing.exists())

    def test_source_projection_blocks_unverified_negative_missing_hash_or_bad_uri(self):
        item = {"bronze_status": "BronzeReady", "verified": True, "run_id": "source-run",
                "manifest_uri": "s3://bucket/manifest.json", "raw_object_uri": "s3://bucket/raw.geojson",
                "sha256": "a" * 64, "manifest_sha256": "b" * 64, "record_count_estimate": 0}
        for update in ({"verified": False}, {"record_count_estimate": -1}, {"manifest_sha256": None},
                       {"manifest_uri": "s3://user:password@bucket/file"}):
            with self.subTest(update=update), self.assertRaises(obs.ObservabilityError):
                obs.source_details("USGS", [{**item, **update}])

    def test_real_protocol_mock_adapter_still_requires_verified_snapshots_and_publication(self):
        # Protocol-only test: no physical Iceberg commit or Trino query is claimed.
        from test_etl_pipeline_runtime import EtlPipelineRuntimeTest
        helper = EtlPipelineRuntimeTest()
        helper.setUp()
        context = helper.real_context()
        context["config_version"] = "1"
        env = {**self.env, "ETL_PHASE_RUNNER_COMMAND": "/test/adapter", "ETL_STAGING_ROOT": self.temp.name + "/etl"}
        upstream = None
        def runner(argv, **kwargs):
            request = json.loads(Path(argv[-1]).read_text())
            receipt = helper.real_result(request["phase"], context, request["upstream"])
            kwargs["stdout"].write(json.dumps(receipt).encode())
            return subprocess.CompletedProcess(argv, 0)
        with patch.object(etl.subprocess, "run", side_effect=runner):
            for phase in etl.PHASES:
                upstream = etl.execute_observed_phase(phase, context, upstream, env)
            etl.observed_publication_summary(context, upstream, env)
        summary = obs.read_summary(context, env)
        self.assertTrue(summary["published"])
        self.assertIsNone(summary["gold_counts"]["published"])
        self.assertEqual("adapter_metadata", summary["evidence_kind"])


if __name__ == "__main__":
    unittest.main()
