import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import mainshock_window_qa as qa
from source_run_guard import lease, SourceRunBusy


class MainshockWindowControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {"SOURCE_GUARD_ROOT": str(self.root / "guard"), "ICEBERG_CATALOG_NAME": "iceberg"}
        self.pins = self.root / "pins.json"
        self.pins.write_text(json.dumps({"bronze_inputs": [{}, {}, {}]}))
        runner = self.root / "runner.jar"
        runner.write_bytes(b"fixture-runner")
        for name, value in (("RUNNER", runner), ("STAGING", self.root / "staging")):
            item = patch.object(qa, name, value)
            item.start()
            self.addCleanup(item.stop)

    @property
    def owner(self):
        return Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json"

    def finished(self, command, **kwargs):
        if command[-1] == "--resume-windows":
            command = command[:-1]
        root = Path(command[-2]).parent
        request = json.loads(Path(command[-2]).read_text())
        lease("assert", {key: request[key] for key in ("dag_id", "run_id")}, kwargs["env"])
        self.assertEqual(self.pins, Path(command[-1]))
        report = {**request, "status": "VERIFIED_PUBLISHED_JMA_WINDOWS", "scope": "BOUNDED_QA_ONLY", "research_ready": False, "rerun_unchanged": True,
                  "trino_microsecond_gate_passed": True,
                  "publication": {"verify_status": "PASSED", "published": True, "publication_id": "pub_" + "a" * 64,
                                  "snapshot_bundle": {"event_current": {"table": request["gold_namespace"] + ".event_current", "snapshot_id": 99, "count": 10}}},
                  "datasets": []}
        mc = '{"applied_mc":2.0}'
        sha = hashlib.sha256(mc.encode()).hexdigest()
        counts = {"input_event_count": 10, "eligible_event_count": 6, "excluded_event_count": 4}
        for split in ("REPRODUCTION", "EXTENSION"):
            common = {"dataset_id": "ds_" + ("b" if split == "REPRODUCTION" else "c") * 64,
                      "code_version": request.get("window_code_version", request["code_version"]),
                      "gold_snapshot_id": 99, "gold_publication_id": "pub_" + "a" * 64, "dataset_status": "BUILDING"}
            artifact = root / split.lower() / "windows" / "candidate.parquet"
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(b"fixture-parquet-checksum-only")
            window = {**common, "research_ready": False, "mc_applied": 2.0, "mc_config_sha256": sha,
                      "mainshock_count": 1, "total_candidate_row_count": 3, "flagged_window_count": 0, "rejected_window_count": 0,
                      "parquet_readback_verified": True, "bucket_join_verified": True,
                      "role_counts": {"PRE": 1, "MAINSHOCK": 1, "POST": 1}, "windows": {"m-one": {"candidate_count": 3}},
                      "parquet_files": [{"path": "windows/candidate.parquet", "bytes": artifact.stat().st_size, "sha256": qa.digest(artifact)}]}
            report["datasets"].append({"manifest": {**common, "dataset_split": split},
                "audit": {**common, "input_mode": "PUBLISHED_SNAPSHOT", "candidate_build_allowed": True, "counts": counts,
                          "mc_applied": 2.0, "mc_config_json": mc, "mc_config_sha256": sha, "exclusion_counts_by_primary_reason": {"ML_BELOW_COMPLETENESS": 4}},
                "window": window, "window_manifest": {**common, **counts, "mc_value": 2.0, "mc_config_sha256": sha, "mainshock_count": 1, "candidate_row_count": 3},
                "trino_oracle": [{"mainshock_event_id": "m-one", "candidate_count": 3, "self_count": 1, "pre_count": 1, "post_count": 1}], "rerun_unchanged": True})
        self.report = report
        (root / "mld-03-report.json").write_text(json.dumps(report))
        return subprocess.CompletedProcess(command, 0)

    def test_resume_uses_only_retained_owner_and_pins_new_builder_code_without_changing_gold_code(self):
        run_id = "mld03-qa-" + "a" * 32
        request = {"run_id": run_id, "dag_id": "mainshock_window_qa", "gold_namespace": "iceberg.gold_qa_" + "a" * 32,
                   "ml_namespace": "iceberg.ml_qa_" + "a" * 32, "code_version": "artifact_" + "b" * 64, "config_version": "1"}
        root = qa.STAGING / run_id
        root.mkdir(parents=True)
        (root / "request.json").write_text(json.dumps(request))
        identity = {key: request[key] for key in ("dag_id", "run_id")}
        lease("acquire", identity, self.env)
        with patch.object(qa.subprocess, "run", side_effect=self.finished) as submit:
            result = qa.resume(run_id, self.pins, self.env)
        self.assertEqual("--resume-windows", submit.call_args.args[0][-1])
        self.assertEqual(request, json.loads((root / "request.json").read_text()))
        saved = json.loads((Path(result["report_path"]).parent / "request.json").read_text())
        self.assertNotEqual(saved["window_code_version"], saved["code_version"])
        self.assertFalse(self.owner.exists())
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(SourceRunBusy):
            qa.resume(run_id, self.pins, self.env)
        submit.assert_not_called()

    def test_success_reads_artifact_checksums_and_releases_own_lease(self):
        with patch.object(qa.subprocess, "run", side_effect=self.finished):
            self.assertEqual("VERIFIED_PUBLISHED_JMA_WINDOWS", qa.run(self.pins, self.env)["status"])
        self.assertFalse(self.owner.exists())

    def test_busy_owner_never_overwritten(self):
        identity = {"dag_id": "daily", "run_id": "other"}
        lease("acquire", identity, self.env)
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(SourceRunBusy):
            qa.run(self.pins, self.env)
        submit.assert_not_called()
        self.assertEqual(identity, json.loads(self.owner.read_text()))

    def test_timeout_or_failed_java_retain_lease(self):
        for outcome in (subprocess.TimeoutExpired("spark-submit", 3600), subprocess.CompletedProcess([], 1)):
            effect = outcome if isinstance(outcome, Exception) else lambda *args, **kwargs: outcome
            with self.subTest(outcome=type(outcome).__name__), patch.object(qa.subprocess, "run", side_effect=effect), self.assertRaises(Exception):
                qa.run(self.pins, self.env)
            self.assertTrue(self.owner.exists())
            lease("release", json.loads(self.owner.read_text()), self.env)  # temporary test state only

    def test_missing_scope_stops_before_acquisition(self):
        self.pins.write_text('{"bronze_inputs":[]}')
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(ValueError):
            qa.run(self.pins, self.env)
        submit.assert_not_called()
        self.assertFalse(self.owner.exists())

    def test_tampered_parquet_receipt_retains_lease(self):
        def corrupted(command, **kwargs):
            result = self.finished(command, **kwargs)
            (Path(command[-2]).parent / "reproduction/windows/candidate.parquet").write_bytes(b"changed")
            return result
        with patch.object(qa.subprocess, "run", side_effect=corrupted), self.assertRaises(RuntimeError):
            qa.run(self.pins, self.env)
        self.assertTrue(self.owner.exists())

    def test_bad_pins_nonfinite_mc_and_missing_pre_or_oracle_are_rejected(self):
        with patch.object(qa.subprocess, "run", side_effect=self.finished):
            result = qa.run(self.pins, self.env)
        root = Path(result["report_path"]).parent
        request = json.loads((root / "request.json").read_text())
        for key, value in (("gold_snapshot_id", 100), ("mc_applied", float("nan")), ("mainshock_count", 0),
                           ("role_counts", {"MAINSHOCK": 1, "POST": 2}), ("parquet_files", [])):
            bad = json.loads(json.dumps(self.report))
            bad["datasets"][0]["window"][key] = value
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                qa.validate_report(bad, request, root)


if __name__ == "__main__":
    unittest.main()
