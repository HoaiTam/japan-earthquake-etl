import json
import copy
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import pipeline_qa_acceptance as qa


class PipelineAcceptanceTest(unittest.TestCase):
    def test_smoke_make_target_never_builds_pulls_or_starts_dependencies(self):
        root = Path(__file__).resolve().parents[2]
        result = qa.subprocess.run(
            ["make", "--dry-run", "smoke-qa-acceptance", "PIPELINE_QA_TASK=QA-02"],
            cwd=root, capture_output=True, text=True, check=True)
        self.assertIn("--component ml --client pipeline-qa-acceptance", result.stdout)
        self.assertIn("run --rm --no-deps --pull never", result.stdout)
        self.assertIn('PIPELINE_QA_TASK="QA-02"', result.stdout)
        for forbidden in ("docker-build", " up ", " start ", " pull ", " init ", "down -v"):
            self.assertNotIn(forbidden, result.stdout)

    def test_foreign_lease_blocks_launch_and_is_not_released(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            foreign = {"dag_id": "daily_etl", "run_id": "foreign-owner"}
            qa.lease("acquire", foreign, env)
            with patch.object(qa, "STAGING", root), patch.object(qa.subprocess, "run") as execute:
                with self.assertRaisesRegex(RuntimeError, "SOURCE_RUN_BUSY"):
                    qa.run("QA-02", env)
            execute.assert_not_called()
            self.assertEqual(json.loads((root / "guard/owner.json").read_text()), foreign)

    def test_only_delivered_tasks_are_available(self):
        self.assertEqual(set(qa.JOBS), {"QA-02", "QA-03"})
        for task in ("QA-01", "QA-04"):
            with self.subTest(task=task), patch.object(qa, "lease") as lease:
                with self.assertRaisesRegex(ValueError, "UNSUPPORTED"):
                    qa.run(task, {})
                lease.assert_not_called()

    def test_matrix_changed_after_execution_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "matrix.json").write_text("{}")
            request = {"matrix_path": str(root / "matrix.json"), "matrix_sha256": "0" * 64}
            with self.assertRaisesRegex(RuntimeError, "MATRIX_CHANGED_AFTER_EXECUTION"):
                qa.validate("QA-02", request, {})

    def test_validation_failure_after_successful_java_releases_only_own_lease(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")

            def execute(command, **kwargs):
                request = json.loads(Path(command[-1]).read_text())
                qa._atomic_json(Path(request["report_path"]), {})
                return SimpleNamespace(returncode=0)

            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", side_effect=execute), \
                    patch.object(qa, "validate", side_effect=RuntimeError("INVALID_REPORT")):
                with self.assertRaisesRegex(RuntimeError, "INVALID_REPORT"):
                    qa.run("QA-02", env)
            self.assertFalse((root / "guard/owner.json").exists())
            self.assertEqual(json.loads(next(root.glob("qa-02/*/lease-release.json")).read_text())["status"], "RELEASED")
            self.assertFalse(list(root.glob("qa-02/*/acceptance.json")))
            execution = json.loads(next(root.glob("qa-02/*/execution.json")).read_text())
            self.assertIn("duration_seconds", execution["capacity"])
            self.assertIn("child_peak_rss_mb", execution["capacity"])
            self.assertEqual(json.loads(next(root.glob("qa-02/*/failure.json")).read_text())["status"], "REPORT_VALIDATION_FAILED")

    def test_acceptance_is_written_only_after_exact_owner_release(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")

            def execute(command, **kwargs):
                request = json.loads(Path(command[-1]).read_text())
                qa._atomic_json(Path(request["report_path"]), {"summary": {"case_count": 8, "passed": 8, "failed": 0}})
                return SimpleNamespace(returncode=0)

            # Control-flow unit test only: publication validation is covered below
            # and by the opt-in runtime command, not mocked acceptance evidence.
            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", side_effect=execute), patch.object(qa, "validate"):
                result = qa.run("QA-02", env)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["lease_release"]["run_id"], result["run_id"])
            self.assertFalse((root / "guard/owner.json").exists())
            self.assertEqual(json.loads(next(root.glob("qa-02/*/acceptance.json")).read_text()), result)

    def test_postrun_validator_requires_existing_output_not_prerun_nonexistence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            matrix = json.loads((Path(qa.__file__).parent / "fixtures/qa_02_rerun_revision_matrix.json").read_text())
            (root / "matrix.json").write_text(json.dumps(matrix))
            (root / "report.json").write_text("{}")
            request = {"namespace": "iceberg.qa02_" + "a" * 32, "matrix_path": str(root / "matrix.json"), "report_path": str(root / "report.json"),
                       "matrix_sha256": qa.hashlib.sha256((root / "matrix.json").read_bytes()).hexdigest()}
            with patch.object(qa.qa02, "_validate_runtime_request", side_effect=AssertionError("pre-run guard used after run")), \
                    patch.object(qa.qa02, "_validate_runtime_report") as validate:
                qa.validate("QA-02", request, {"cases": []})
                validate.assert_called_once()

    def test_unknown_task_cannot_acquire_lease_or_spawn(self):
        with patch.object(qa, "lease") as lease, patch.object(qa.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "UNSUPPORTED"):
                qa.run("main", {})
            lease.assert_not_called()
            run.assert_not_called()

    def test_nonzero_java_exit_keeps_lease_and_never_writes_acceptance(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", return_value=SimpleNamespace(returncode=2)):
                with self.assertRaisesRegex(RuntimeError, "FAILED_CHECK_SCOPED"):
                    qa.run("QA-02", env)
            owner = json.loads((root / "guard/owner.json").read_text())
            self.assertTrue(owner["run_id"].startswith("qa02-runtime-"))
            self.assertFalse(list(root.glob("qa-02/*/acceptance.json")))
            self.assertEqual(json.loads(next(root.glob("qa-02/*/failure.json")).read_text())["status"], "FAILED_LEASE_RETAINED")

    def test_java_timeout_keeps_exact_lease(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", side_effect=qa.subprocess.TimeoutExpired("fixed-java", 3600)):
                with self.assertRaisesRegex(RuntimeError, "TIMEOUT_CHECK_PROCESS"):
                    qa.run("QA-02", env)
            self.assertTrue((root / "guard/owner.json").is_file())
            self.assertFalse(list(root.glob("qa-02/*/acceptance.json")))

    def test_nonzero_exit_keeps_lease_even_when_execution_evidence_cannot_be_written(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            write = qa._atomic_json

            def evidence(path, value):
                if path.name == "execution.json":
                    raise OSError("CONTROLLED_EVIDENCE_WRITE_FAILURE")
                return write(path, value)

            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", return_value=SimpleNamespace(returncode=2)), \
                    patch.object(qa, "_atomic_json", side_effect=evidence):
                with self.assertRaisesRegex(OSError, "CONTROLLED_EVIDENCE_WRITE_FAILURE"):
                    qa.run("QA-02", env)
            self.assertTrue((root / "guard/owner.json").exists())
            self.assertFalse(list(root.glob("qa-02/*/lease-release.json")))
            self.assertFalse(list(root.glob("qa-02/*/acceptance.json")))



class PublicationGuardTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.namespace = "iceberg.qa02_" + "a" * 32
        self.operation = "qa-operation"
        work = self.root / self.operation
        work.mkdir()
        receipts = []
        for phase in qa.etl.PHASES:
            artifact = {"phase": phase}
            (work / (phase + ".json")).write_text(json.dumps(artifact))
            receipts.append({"artifact_sha256": qa.etl.digest(artifact)})
        tables = {name: {"table": self.namespace + "." + name, "snapshot_id": 4691854184265549327, "count": 1}
                  for name in sorted(qa.etl.TABLES)}
        commit = {"namespace": self.namespace, "gold_run_id": self.operation, "tables": tables,
                  "status": "COMMITTED", "published": False, "identity_sha256": "b" * 64}
        checks = {name: {"actual": 0, "expected": 0} for name in qa.GOLD_BLOCKER_CHECKS}
        checks.update({kind + ":" + name: {"actual": 1, "expected": 1}
                       for name in tables for kind in ("count", "identity")})
        self.record = {
            "context": {"resolver_request": {"namespace": self.namespace}, "operation_id": self.operation},
            "operation_id": self.operation, "receipts": receipts, "commit": commit,
            "silver": {"reconciliation": {"balanced": True}},
            "publication": {"engine": "TRINO", "verify_status": "PASSED", "published": True,
                            "gold_run_id": self.operation, "namespace": self.namespace,
                            "identity_sha256": "b" * 64, "snapshot_bundle": tables, "checks": checks},
        }
        self.repin()
        # Existing production context/receipt contracts have their own tests.
        # These tests exercise the additional artifact/snapshot/Trino guards.
        for name in ("validate_context", "validate_receipt"):
            mock = patch.object(qa.etl, name)
            mock.start()
            self.addCleanup(mock.stop)

    def repin(self):
        digest = qa.hashlib.sha256(json.dumps(self.record["commit"], separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        self.record["publication"].update(bundle_sha256=digest, publication_id="pub_" + digest)
        for phase, field in (("silver", "silver"), ("gold", "commit"), ("publish", "publication")):
            artifact = self.record[field]
            (self.root / self.operation / (phase + ".json")).write_text(json.dumps(artifact))
            self.record["receipts"][qa.etl.PHASES.index(phase)]["artifact_sha256"] = qa.etl.digest(artifact)
        snapshots = {name: pin["snapshot_id"] for name, pin in self.record["commit"]["tables"].items()}
        for receipt in self.record["receipts"][3:]:
            receipt["snapshots"] = snapshots
        self.record["receipts"][-1]["publication_id"] = "pub_" + digest

    def check(self):
        qa.validate_publication(self.record, self.namespace, self.root)

    def test_six_exact_snapshots_and_all_trino_checks_are_accepted(self):
        self.check()

    def test_missing_phase_is_rejected(self):
        self.record["receipts"].pop()
        with self.assertRaisesRegex(RuntimeError, "PHASES_INCOMPLETE"):
            self.check()

    def test_changed_phase_artifact_is_rejected(self):
        (self.root / self.operation / (qa.etl.PHASES[0] + ".json")).write_text("{}")
        with self.assertRaisesRegex(RuntimeError, "ARTIFACT_CHANGED"):
            self.check()

    def test_missing_uniqueness_gate_is_rejected(self):
        del self.record["publication"]["checks"]["unique:event_current"]
        with self.assertRaisesRegex(RuntimeError, "PIN_OR_CHECKS_MISSING"):
            self.check()

    def test_different_published_bundle_is_rejected(self):
        self.record["publication"]["snapshot_bundle"] = {}
        with self.assertRaisesRegex(RuntimeError, "PUBLICATION_INVALID"):
            self.check()

    def test_forged_commit_hash_is_rejected(self):
        self.record["publication"]["bundle_sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "PIN_OR_CHECKS_MISSING"):
            self.check()

    def test_boolean_or_out_of_range_snapshot_is_rejected(self):
        for value in (True, 0, 9223372036854775808, "latest"):
            with self.subTest(value=value):
                self.record["commit"]["tables"]["event_current"]["snapshot_id"] = value
                self.repin()
                with self.assertRaisesRegex(RuntimeError, "READBACK_INVALID"):
                    self.check()

    def test_trino_count_mismatch_is_rejected(self):
        self.record["publication"]["checks"]["count:event_current"]["actual"] = 2
        with self.assertRaisesRegex(RuntimeError, "READBACK_INVALID"):
            self.check()

    def test_nonzero_blocker_cannot_pass_by_changing_expected_value(self):
        self.record["publication"]["checks"]["required_fields"] = {"actual": 1, "expected": 1}
        with self.assertRaisesRegex(RuntimeError, "BLOCKER_EXPECTATION_CHANGED"):
            self.check()

    def test_unbalanced_silver_is_rejected(self):
        self.record["silver"]["reconciliation"]["balanced"] = False
        with self.assertRaisesRegex(RuntimeError, "COUNTS_UNBALANCED"):
            self.check()

    def test_record_cannot_substitute_another_valid_phase_artifact(self):
        self.record["silver"]["unbound_field"] = "changed"
        with self.assertRaisesRegex(RuntimeError, "RECORD_ARTIFACT_CHANGED"):
            self.check()

    def test_receipt_snapshots_must_equal_exact_commit(self):
        self.record["receipts"][3]["snapshots"] = {}
        with self.assertRaisesRegex(RuntimeError, "RECEIPT_SNAPSHOT_CHANGED"):
            self.check()

    def test_receipt_publication_id_must_equal_verified_publication(self):
        self.record["receipts"][-1]["publication_id"] = "pub_" + "0" * 64
        with self.assertRaisesRegex(RuntimeError, "RECEIPT_PUBLICATION_CHANGED"):
            self.check()


class HistoricalGuardTest(unittest.TestCase):
    """Small metadata fixtures, not runtime acceptance or public earthquake payloads."""
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.captures = self.root / "captures"
        self.captures.mkdir()
        self.pins = {}
        for name in sorted(qa.HISTORY_FILES):
            data = ("metadata-test-" + name).encode()
            (self.captures / name).write_bytes(data)
            self.pins[name] = qa.hashlib.sha256(data).hexdigest()
        (self.captures / "capture-pins.json").write_text(json.dumps(self.pins))
        mock = patch.object(qa, "CAPTURES", self.captures)
        mock.start()
        self.addCleanup(mock.stop)
        inventory = patch.object(qa, "INVENTORY", Path(__file__).resolve().parents[2] / "config/jma/hypocenter_archives_v1.csv")
        inventory.start()
        self.addCleanup(inventory.stop)
        matrix = json.loads((Path(qa.__file__).parent / "fixtures/qa_03_jma_historical_matrix.json").read_text())
        self.namespace = "iceberg.qa03_" + "a" * 32
        matrix["input_pins"]["namespace"] = self.namespace
        (self.root / "matrix.json").write_text(json.dumps(matrix))
        (self.root / "report.json").write_text("{}")
        self.request = {"run_id": "qa03-test", "namespace": self.namespace,
                        "matrix_path": str(self.root / "matrix.json"), "report_path": str(self.root / "report.json"),
                        "matrix_sha256": qa.hashlib.sha256((self.root / "matrix.json").read_bytes()).hexdigest(),
                        "inventory_sha256": matrix["inventory_sha256"], "capture_pins_sha256": qa.history_pins()[1]}
        source = {name: {"source_system": "JMA_BULLETIN", "sha256": checksum, "catalog_release": "release-v1"}
                  for name, checksum in self.pins.items() if name.endswith(".zip")}
        source["h199701.zip"].update(year=1997, segment="jan-sep")
        usgs = {"source_system": "USGS", "sha256": self.pins["usgs-2023-09.geojson"]}
        changed_pin = dict(source["h199701.zip"], sha256="f" * 64, catalog_release="release-v2")
        inputs = [[source["h2000.zip"]], [source["h199701.zip"], source["h199710.zip"]],
                  [source["h199710.zip"]], [source["h2000.zip"]], [source["h2023.zip"], usgs], [changed_pin]]
        months = ["2000-02", "1997-09", "1997-10", "2000-01", "2023-09", "1997-09"]
        records = []
        for index, (pins, month) in enumerate(zip(inputs, months)):
            records.append({"operation_id": "op" + str(index), "context": {"resolver_request": {
                "namespace": self.namespace, "target_months": [month], "bronze_inputs": pins,
                "source_cutoff_utc": "2026-10-21T00:00:00Z"}}, "silver": {"affected_months": [month]},
                "receipts": [{"phase": name} for name in qa.etl.PHASES],
                "commit": {"tables": {name: {"snapshot_id": index + 1, "count": 1} for name in qa.etl.TABLES}}})
        snapshots = lambda index: {name: index + 1 for name in qa.etl.TABLES}
        stop = {"reason": "QA_STOP_AFTER_DURABLE_SILVER", "exception_class": "java.io.IOException", "process_restart": False,
                "downstream_blocked": True, "context": records[1]["context"], "completed_receipts": records[1]["receipts"][:3],
                "gold_baselines": snapshots(0)}
        (self.root / "resume-stop.json").write_text(json.dumps(stop))
        conflict = {"resolver_request": {"namespace": self.namespace, "bronze_inputs": [dict(changed_pin, sha256=self.pins["h199701.zip"])]}}
        self.report = {"task": "QA-03", "schema_version": "qa03-storage-acceptance-v1", "status": "PASS", "run_id": "qa03-test",
                       "namespace": self.namespace, "matrix_sha256": self.request["matrix_sha256"],
                       "inventory_sha256": matrix["inventory_sha256"], "capture_pins_sha256": self.request["capture_pins_sha256"],
                       "capture_sha256": self.pins, "original_public_captures": True, "controlled_release_is_synthetic": True,
                       "research_ready": False, "download_count_during_execution": 0,
                       "checks": {name: True for name in qa.qa03.REQUIRED_CASES},
                       "summary": {"case_count": 6, "passed": 6, "failed": 0, "pending_runtime": 0},
                       "source_cutoff_utc": "2026-10-21T00:00:00Z", "completed_at_utc": "2026-10-21T00:01:00Z",
                       "publications": records, "bronze_inputs": source, "usgs_bronze_input": usgs,
                       "case_evidence": {
                           "resume_after_failure": stop,
                           "reuse_verified_archive": {"operation_id": "op1", "reused_receipt": records[1]["receipts"][1], "download_count": 0},
                           "changed_release": {"before_operation_id": "op4", "after_operation_id": "op5", "old_pin": source["h199701.zip"],
                                               "new_pin": changed_pin, "synthetic": True, "old_snapshot_readback": True},
                           "same_release_different_bytes": {"reason": "JMA_RELEASE_IDENTITY_OR_ORDER_CONFLICT", "downstream_blocked": True,
                                                            "context": conflict, "baselines_before": snapshots(5), "baselines_after": snapshots(5)},
                           "resource_guard": {"reason": "BOUNDED_EXACT_MONTHS_REQUIRED", "recovery_operation_id": "op5",
                                              "baselines_before": snapshots(5), "baselines_after": snapshots(5),
                                              "resolver_request": {"target_months": list(range(25)), "namespace": self.namespace}},
                           "retention_outside_scope": {"month": "2000-02", "before_operation_id": "op0", "after_operation_id": "op5",
                                                       "old_snapshot_readback": True, "baseline_count": 1, "before_sha256": "b" * 64, "after_sha256": "b" * 64}}}

    def check(self):
        # Publication/context protocol has independent tests; this suite tests
        # additional capture, scope and recovery evidence bindings only.
        with patch.object(qa, "validate_publication") as publications, patch.object(qa.etl, "validate_context"):
            qa.validate("QA-03", self.request, self.report)
            self.assertEqual(publications.call_count, 6)

    def test_complete_metadata_is_accepted(self):
        self.check()

    def test_java_nanoseconds_are_validated_on_microsecond_only_python(self):
        self.report["completed_at_utc"] = "2026-10-21T00:01:00.153849258Z"
        parser = qa.qa03._utc_instant

        def older_python(value):
            self.assertNotRegex(value, r"\.\d{7,}Z$")
            return parser(value)

        with patch.object(qa.qa03, "_utc_instant", side_effect=older_python):
            self.check()
        self.assertEqual(self.report["completed_at_utc"], "2026-10-21T00:01:00.153849258Z")

    def test_completion_requires_bounded_utc_instant(self):
        for value in ("2026-10-21T00:01:00+00:00", "2026-10-21T00:01:00.1234567890Z", "2026-99-21T00:01:00Z"):
            with self.subTest(value=value):
                self.report["completed_at_utc"] = value
                with self.assertRaises((RuntimeError, qa.qa03.Qa03Error)):
                    self.check()

    def test_boolean_pass_flags_are_not_enough(self):
        self.report["case_evidence"] = {}
        with self.assertRaisesRegex(RuntimeError, "EVIDENCE_MISSING"):
            self.check()

    def test_publication_validator_failure_is_not_swallowed(self):
        with patch.object(qa, "validate_publication", side_effect=RuntimeError("INVALID_PUBLICATION")):
            with self.assertRaisesRegex(RuntimeError, "INVALID_PUBLICATION"):
                qa.validate("QA-03", self.request, self.report)

    def test_changed_capture_bytes_cannot_refresh_pins(self):
        (self.captures / "h2000.zip").write_bytes(b"different")
        with self.assertRaisesRegex(RuntimeError, "CAPTURE_PIN_CHANGED"):
            self.check()

    def test_capture_symlink_is_rejected(self):
        (self.captures / "h2000.zip").unlink()
        (self.captures / "h2000.zip").symlink_to(self.root / "report.json")
        with self.assertRaises(qa.qa03.Qa03Error):
            self.check()

    def test_matrix_drift_is_rejected(self):
        with (self.root / "matrix.json").open("a") as stream:
            stream.write("\n")
        with self.assertRaisesRegex(RuntimeError, "PINS_CHANGED"):
            self.check()

    def test_missing_or_duplicate_publication_is_rejected(self):
        self.report["publications"][-1] = copy.deepcopy(self.report["publications"][0])
        with self.assertRaisesRegex(RuntimeError, "PUBLICATIONS_INCOMPLETE"):
            self.check()

    def test_scope_expansion_is_rejected(self):
        self.report["publications"][1]["silver"]["affected_months"].append("1997-08")
        with self.assertRaisesRegex(RuntimeError, "SCOPE_CHANGED"):
            self.check()

    def test_context_cannot_substitute_an_unpinned_capture(self):
        self.report["publications"][3]["context"]["resolver_request"]["bronze_inputs"] = []
        with self.assertRaisesRegex(RuntimeError, "CONTEXT_CAPTURE_CHANGED"):
            self.check()

    def test_stop_receipt_must_equal_resumed_silver(self):
        self.report["case_evidence"]["resume_after_failure"]["completed_receipts"] = []
        with self.assertRaisesRegex(RuntimeError, "RESUME_EVIDENCE_INVALID"):
            self.check()

    def test_changed_release_cannot_claim_real_upstream_correction(self):
        self.report["case_evidence"]["changed_release"]["synthetic"] = False
        with self.assertRaisesRegex(RuntimeError, "RELEASE_EVIDENCE_INVALID"):
            self.check()

    def test_rejected_collision_cannot_change_snapshots(self):
        self.report["case_evidence"]["same_release_different_bytes"]["baselines_after"] = {}
        with self.assertRaisesRegex(RuntimeError, "COLLISION_EVIDENCE_INVALID"):
            self.check()

    def test_resource_failure_must_have_recovery_publication(self):
        self.report["case_evidence"]["resource_guard"]["recovery_operation_id"] = "other"
        with self.assertRaisesRegex(RuntimeError, "RESOURCE_EVIDENCE_INVALID"):
            self.check()

    def test_retention_requires_positive_data_not_two_empty_partitions(self):
        self.report["case_evidence"]["retention_outside_scope"]["baseline_count"] = 0
        with self.assertRaisesRegex(RuntimeError, "RETENTION_EVIDENCE_INVALID"):
            self.check()

    def test_sample_is_not_research_ready(self):
        self.report["research_ready"] = True
        with self.assertRaisesRegex(RuntimeError, "REPORT_INVALID"):
            self.check()

    def test_missing_capture_prevents_lease_and_java_launch(self):
        (self.captures / "h2000.zip").unlink()
        env = {"ICEBERG_CATALOG_NAME": "iceberg"}
        with patch.object(qa, "STAGING", self.root), patch.object(qa, "lease") as lease, patch.object(qa.subprocess, "run") as execute:
            with self.assertRaises(qa.qa03.Qa03Error):
                qa.run("QA-03", env)
            lease.assert_not_called()
            execute.assert_not_called()

    def test_history_launch_uses_mounted_inventory_and_releases_exact_owner(self):
        env = {"ICEBERG_CATALOG_NAME": "iceberg", "SOURCE_GUARD_ROOT": str(self.root / "guard")}

        def execute(command, **kwargs):
            self.assertIn(qa.JOBS["QA-03"], command)
            request = json.loads(Path(command[-1]).read_text())
            self.assertEqual(request["inventory_sha256"], self.request["inventory_sha256"])
            self.assertEqual(request["capture_pins_sha256"], self.request["capture_pins_sha256"])
            self.assertEqual(request["request_version"], "qa03-fixed-request-v1")
            qa._atomic_json(Path(request["report_path"]), {"summary": {"case_count": 6, "passed": 6, "failed": 0, "pending_runtime": 0}})
            return SimpleNamespace(returncode=0)

        # Control plane only; this mock is never persisted as runtime evidence.
        with patch.object(qa, "STAGING", self.root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                patch.object(qa.subprocess, "run", side_effect=execute), patch.object(qa, "validate"):
            result = qa.run("QA-03", env)
        self.assertEqual(result["summary"]["passed"], 6)
        self.assertEqual(result["lease_release"]["status"], "RELEASED")
        self.assertFalse((self.root / "guard/owner.json").exists())


if __name__ == "__main__":
    unittest.main()
