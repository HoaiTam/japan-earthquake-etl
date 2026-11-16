"""Offline metadata/controller guards; fixtures are not research acceptance."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import subprocess

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/dat03-portable-import.py"
spec = importlib.util.spec_from_file_location("portable_import", SCRIPT)
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)


class PortableImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.plan = {"owner": "ThanhTris", "affected_months": ["2013-08"]}
        self.receipt = {"plan_sha256": self.write("batch-01-plan.json", self.plan),
                        "gold_published": False, "portable_import_ready": False, "rows_decoded": 2,
                        "groups": [{"run_id": "source-1", "current_observation_count": 2,
                                    "canonical_count": 2, "dataset_counts": {"source_link": 0}}]}
        self.checkpoint = {"previous_checkpoint_sha256": None, "plan_sha256": self.receipt["plan_sha256"],
                           "receipt_sha256": self.write("batch-01-readback.json", self.receipt),
                           "config_sha256": "a" * 64, "accepted_readback_months": ["2013-08"], "rows_decoded": 2}
        self.completed = {"months_verified": 1, "gold_published": False, "batch_count": 1,
                          "config_sha256": "a" * 64, "rows_decoded": 2,
                          "last_checkpoint_sha256": self.write("batch-01-checkpoint.json", self.checkpoint)}
        self.worker = {"owner": "ThanhTris", "readback_root": str(self.root),
                       "start_month": "2013-08", "end_month": "2013-08"}
        self.complete()

    def write(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value))
        return app.sha(path)

    def complete(self):
        self.worker["completed_sha256"] = self.write("completed.json", self.completed)

    def test_exact_checkpoint_counts_and_original_plan_are_retained(self):
        batch = app.worker_batches(self.worker)[0]
        self.assertEqual(batch["months"], ["2013-08"])
        self.assertEqual(batch["observation_count"], 2)
        self.assertEqual(batch["run_ids"], ["source-1"])
        self.assertEqual(batch["original_plan_sha256"], app.sha(self.root / "batch-01-plan.json"))

    def test_changed_completed_root_is_rejected(self):
        self.write("completed.json", {**self.completed, "rows_decoded": 3})
        with self.assertRaisesRegex(ValueError, "PIN_MISMATCH"):
            app.worker_batches(self.worker)

    def test_changed_plan_is_rejected(self):
        self.write("batch-01-plan.json", {**self.plan, "owner": "rosy179"})
        with self.assertRaisesRegex(ValueError, "CHECKPOINT_CHAIN_CHANGED"):
            app.worker_batches(self.worker)

    def test_previous_checkpoint_must_be_exact(self):
        self.checkpoint["previous_checkpoint_sha256"] = "b" * 64
        self.completed["last_checkpoint_sha256"] = self.write("batch-01-checkpoint.json", self.checkpoint)
        self.complete()
        with self.assertRaisesRegex(ValueError, "CHECKPOINT_CHAIN_CHANGED"):
            app.worker_batches(self.worker)

    def test_missing_month_is_not_inferred_from_directory(self):
        self.worker["end_month"] = "2013-09"
        self.completed["months_verified"] = 2
        self.complete()
        with self.assertRaisesRegex(ValueError, "EXACT_COVERAGE"):
            app.worker_batches(self.worker)

    def test_count_tamper_is_not_accepted(self):
        self.checkpoint["rows_decoded"] = 3
        self.completed["last_checkpoint_sha256"] = self.write("batch-01-checkpoint.json", self.checkpoint)
        self.complete()
        with self.assertRaisesRegex(ValueError, "COUNTS_CHANGED"):
            app.worker_batches(self.worker)

    def test_existing_output_is_rejected_before_runtime(self):
        with self.assertRaisesRegex(ValueError, "NEW_EXACT_IMPORT_ROOT"):
            app.prepare({"version": "dat03-portable-import-preparation-v1"}, self.root)

    def test_worker_bounds_are_checked_before_inputs_or_docker(self):
        for seconds in (0, 599, 6001, True):
            with self.assertRaisesRegex(ValueError, "BOUNDED_IMPORT_WORKER"):
                app.worker(Path("/opt/pipeline/staging/dat03-portable-import/" + "a" * 32), "b" * 64, "c" * 32, seconds)

    def test_completed_rerun_must_keep_trusted_host_catalog_sha(self):
        previous = [{"status": "PREPARED", "catalog_sha256": "a" * 64}]
        current = {"status": "PREPARED", "catalog_sha256": "a" * 64, "reused": True}
        app.verify_completion_reuse(previous, current)
        for change in ({"catalog_sha256": "b" * 64}, {"reused": False}, {"status": "CHECKPOINTED"}):
            with self.assertRaisesRegex(ValueError, "TRUSTED_COMPLETED_CATALOG_CHANGED"):
                app.verify_completion_reuse(previous, {**current, **change})

    def test_unknown_or_failed_host_session_is_never_auto_resumed(self):
        sessions = self.root / "host-sessions"
        sessions.mkdir()
        (sessions / ("a" * 32)).mkdir()
        with self.assertRaisesRegex(ValueError, "REQUIRES_ACCEPTED_PREVIOUS_SESSIONS"):
            app.accepted_sessions(self.root, {})

    def test_host_capture_binds_exact_execution_and_session_report(self):
        (self.root / "runtime").mkdir()
        (self.root / "metadata").mkdir()
        report = {"status": "CHECKPOINTED", "completed_batches": 1}
        candidate = {"execution_sha256": self.write("runtime/execution.json", {"exit_code": 0}),
                     "report_sha256": self.write("metadata/session-report.json", report), "report": report}
        app.verify_host_capture(self.root, candidate)
        self.write("metadata/session-report.json", {**report, "completed_batches": 2})
        with self.assertRaisesRegex(ValueError, "EXECUTION_REPORT_CAPTURE_CHANGED"):
            app.verify_host_capture(self.root, candidate)

    def test_optional_progress_timeout_does_not_launch_retry_or_fail_worker(self):
        with patch.object(app.subprocess, "run", side_effect=subprocess.TimeoutExpired("docker", 20)) as command:
            self.assertIsNone(app.progress_line("/opt/pipeline/staging/dat03-portable-import/" + "a" * 32, "b" * 32))
            self.assertEqual(command.call_count, 1)

    def test_capture_refuses_unstaged_root_without_launching_job(self):
        with patch.object(app.subprocess, "run") as command:
            with self.assertRaises(Exception): app.capture(self.root, "c" * 32, {})
            command.assert_not_called()


if __name__ == "__main__":
    unittest.main()
