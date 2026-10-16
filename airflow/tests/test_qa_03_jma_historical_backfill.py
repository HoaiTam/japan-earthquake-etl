import hashlib
import json
import os
from pathlib import Path
import shutil
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import qa_03_jma_historical_backfill as qa03
from qa_03_jma_historical_backfill import Qa03Error, load_matrix, prepare_report, run_live, write_preparation


MATRIX = Path(__file__).parents[1] / "dags/fixtures/qa_03_jma_historical_matrix.json"
INVENTORY = Path(__file__).parents[2] / "config/jma/hypocenter_archives_v1.csv"
SHA = "a" * 64


class Qa03JmaHistoricalBackfillTest(unittest.TestCase):
    def test_matrix_computes_jst_archive_halo_and_prepared_report(self):
        matrix = load_matrix(MATRIX)
        self.assertEqual("qa-03-v2", matrix["matrix_version"])
        september = next(item for item in matrix["_computed_coverage"] if item["month"] == "1997-09")
        self.assertEqual(["h199701.zip", "h199710.zip"], september["required_archives"])
        self.assertEqual("1997-08-31T23:59:44Z", september["halo_start_utc"])
        self.assertEqual("1997-10-01T00:00:16Z", september["halo_end_utc"])
        report = prepare_report(MATRIX)
        self.assertEqual("PREPARED", report["status"])
        self.assertFalse(report["runtime_complete"])
        self.assertIsNone(report["capacity"]["duration_seconds"])
        self.assertIsNone(report["capacity"]["resource_failures"])

    def test_preparation_writes_identity_coverage_and_capacity_note(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = write_preparation(MATRIX, root / "qa-03.json", root / "qa-03.md")
            self.assertEqual(report, json.loads((root / "qa-03.json").read_text(encoding="utf-8")))
            markdown = (root / "qa-03.md").read_text(encoding="utf-8")
            self.assertIn("h199701.zip, h199710.zip", markdown)
            self.assertIn("Capacity fields are null", markdown)
            self.assertIn(report["inventory_sha256"], markdown)

    def _mutated_matrix(self, change, expected_reason):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
            change(matrix)
            path.write_text(json.dumps(matrix), encoding="utf-8")
            with self.assertRaisesRegex(Qa03Error, expected_reason):
                load_matrix(path)

    def test_swapped_months_wrong_segment_and_invalid_month_are_rejected(self):
        def swap(matrix):
            matrix["scope"][2]["months"] = ["2023-09"]
            matrix["scope"][3]["months"] = ["2000-01"]
        self._mutated_matrix(swap, "QA03_SCOPE_YEAR_MONTH_MISMATCH")
        self._mutated_matrix(lambda m: m["scope"][2].update(months=["2000-13"]), "QA03_MONTH_INVALID")
        self._mutated_matrix(lambda m: m["scope"][0].update(segment="oct-dec"), "QA03_SCOPE_ARCHIVE_IDENTITY_MISMATCH")

    def test_missing_1997_archive_halo_is_rejected(self):
        self._mutated_matrix(lambda m: m["scope"][0].update(support_archives=["h199701.zip"]),
                             "QA03_ARCHIVE_HALO_COVERAGE_MISMATCH")

    def test_missing_or_duplicate_case_and_operation_is_rejected(self):
        self._mutated_matrix(lambda m: m["cases"].pop(), "QA03_CASES_REQUIRED")
        self._mutated_matrix(lambda m: m["cases"][1].update(operation_id=m["cases"][0]["operation_id"]), "QA03_CASE_INVALID")

    def _runtime_fixture(self, root: Path):
        matrix_path = root / "matrix.json"
        shutil.copyfile(MATRIX, matrix_path)
        inventory_path = root / "inventory.csv"
        shutil.copyfile(INVENTORY, inventory_path)
        request_path = root / "request.json"
        report_path = root / "report.json"
        matrix = load_matrix(matrix_path, inventory_path)
        archive_names = sorted({name for entry in matrix["_computed_coverage"] for name in entry["required_archives"]})
        input_refs = [{"archive_name": name, "uri": "s3://qa-03/input/" + name + "/manifest.json",
                       "sha256": hashlib.sha256(("manifest:" + name).encode()).hexdigest(),
                       "raw_sha256": hashlib.sha256(("raw:" + name).encode()).hexdigest(),
                       "catalog_release": "release-" + name} for name in archive_names]
        request = {
            "task_id": "QA-03", "request_version": "qa-03-runtime-v1", "run_id": "qa03-run-001",
            "operation_id": "qa03-operation-001", "namespace": "qa-03-jma-backfill",
            "matrix_path": str(matrix_path), "matrix_sha256": matrix["_matrix_sha256"],
            "inventory_path": str(inventory_path), "inventory_sha256": matrix["inventory_sha256"],
            "source_cutoff_utc": "2026-10-17T00:00:00Z", "input_refs": input_refs,
            "report_path": str(report_path),
        }
        request_path.write_text(json.dumps(request), encoding="utf-8")
        identity = {"run_id": request["run_id"], "operation_id": request["operation_id"],
                    "namespace": request["namespace"], "matrix_sha256": request["matrix_sha256"],
                    "inventory_sha256": request["inventory_sha256"], "source_cutoff_utc": request["source_cutoff_utc"]}
        report = {"task_id": "QA-03", "report_version": "qa-03-report-v2", "status": "PASS",
                  "runtime_complete": True, "identity": identity, "verified_input_refs": request["input_refs"],
                  "scope": matrix["_selected_scope"], "coverage": [], "cases": [],
                  "capacity": {"duration_seconds": 12.5, "peak_memory_mb": 42.0, "download_count": 1,
                               "download_reused": 3, "verified_archive_reused": 3, "resource_failures": 1},
                  "summary": {"scope_count": 4, "case_count": 6, "passed": 6, "failed": 0, "pending_runtime": 0},
                  "snapshots": {"bootstrap": False, "refs": [{"table": "silver.events", "snapshot_id": 11}]}}
        for coverage in matrix["_computed_coverage"]:
            report["coverage"].append({**coverage, "source_cutoff_utc": request["source_cutoff_utc"],
                "verified_archives": [{"archive_name": name, "verified": True,
                    "catalog_release": next(ref["catalog_release"] for ref in request["input_refs"] if ref["archive_name"] == name),
                    "manifest_sha256": next(ref["sha256"] for ref in request["input_refs"] if ref["archive_name"] == name),
                    "raw_sha256": next(ref["raw_sha256"] for ref in request["input_refs"] if ref["archive_name"] == name),
                    "input_ref_sha256": next(ref["sha256"] for ref in request["input_refs"] if ref["archive_name"] == name)}
                    for name in coverage["required_archives"]]})
        assertions_by_case = {
            "resume_after_failure": {"same_verified_archive": True, "duplicate_count_delta": 0, "readback": True},
            "reuse_verified_archive": {"archive_sha256_before": SHA, "archive_sha256_after": SHA, "download_count": 0, "verification_retained": True},
            "changed_release": {"old_catalog_release": "old", "new_catalog_release": "new", "old_snapshot_readable": True, "old_snapshot_id": 10, "new_snapshot_id": 11},
            "same_release_different_bytes": {"rejected": True, "downstream_blocked": True, "data_unchanged": True, "old_sha256": SHA, "new_sha256": "b" * 64},
            "retention_outside_scope": {"outside_scope_before": {"partition": "2001-01", "count": 4, "content_sha256": "c" * 64}, "outside_scope_after": {"partition": "2001-01", "count": 4, "content_sha256": "c" * 64}, "snapshots": {"bootstrap": False, "refs": [{"table": "gold.events", "snapshot_id": 12}]}},
            "resource_guard": {"batch_months": ["1997-09"], "guard_triggered": True, "recovery_passed": True, "scope_months": ["1997-09"]},
        }
        evidence_payloads = {}
        for case in matrix["cases"]:
            case_id = case["case_id"]
            assertions = assertions_by_case[case_id]
            before = {"bootstrap": True, "refs": []}
            after = {"bootstrap": False, "refs": [{"table": "silver.events", "snapshot_id": 11}]}
            filename = case_id + ".json"
            evidence = {"identity": identity, "case_id": case_id, "operation_id": case["operation_id"],
                        "scope": matrix["_selected_scope"], "input_refs": request["input_refs"],
                        "assertions": assertions, "snapshots_before": before, "snapshots_after": after}
            evidence_bytes = (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()
            evidence_payloads[filename] = evidence_bytes
            report["cases"].append({"case_id": case_id, "operation_id": case["operation_id"], "status": "PASS",
                                    "assertions": assertions, "snapshots_before": before, "snapshots_after": after,
                                    "evidence": {"file": filename, "sha256": hashlib.sha256(evidence_bytes).hexdigest()}})
        return request_path, report_path, report, evidence_payloads

    def _write_fixture_output(self, report_path, report, evidence_payloads):
        evidence_dir = report_path.parent / (report_path.name + ".evidence")
        evidence_dir.mkdir()
        for filename, content in evidence_payloads.items():
            (evidence_dir / filename).write_bytes(content)
        report_path.write_text(json.dumps(report), encoding="utf-8")

    def test_positive_runtime_report_and_subprocess_route(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path, report_path, report, evidence_payloads = self._runtime_fixture(root)
            runner_path = root / "qa03_fixture_runner.py"
            runner_path.write_text(
                "import json, sys\nfrom pathlib import Path\n"
                "request=json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))\n"
                "report=" + repr(report) + "\n"
                "evidence=" + repr({name: content.decode("utf-8") for name, content in evidence_payloads.items()}) + "\n"
                "target=Path(request['report_path'])\n"
                "folder=target.parent/(target.name+'.evidence')\nfolder.mkdir()\n"
                "[(folder/name).write_bytes(body.encode('utf-8')) for name,body in evidence.items()]\n"
                "target.write_text(json.dumps(report), encoding='utf-8')\n",
                encoding="utf-8",
            )
            command = shlex.join([sys.executable, str(runner_path)])
            result = run_live(request_path, {**os.environ, "QA_03_RUNTIME_COMMAND": command,
                                             "QA_03_RUNTIME_TIMEOUT_SECONDS": "15"})
            self.assertEqual("PASS", result["status"])
            self.assertTrue(report_path.is_file())

    def test_malformed_request_is_rejected_before_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path, _, _, _ = self._runtime_fixture(root)
            request = json.loads(request_path.read_text(encoding="utf-8"))
            request.pop("report_path")
            request_path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(qa03.subprocess, "run") as run:
                with self.assertRaisesRegex(Qa03Error, "QA03_REPORT_REFERENCE_INVALID"):
                    run_live(request_path, {"QA_03_RUNTIME_COMMAND": "fixture"})
                run.assert_not_called()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path, _, _, _ = self._runtime_fixture(root)
            request = json.loads(request_path.read_text(encoding="utf-8"))
            request["matrix_sha256"] = "0" * 64
            request_path.write_text(json.dumps(request), encoding="utf-8")
            with patch.object(qa03.subprocess, "run") as run:
                with self.assertRaisesRegex(Qa03Error, "QA03_MATRIX_CHECKSUM_MISMATCH"):
                    run_live(request_path, {"QA_03_RUNTIME_COMMAND": "fixture"})
                run.assert_not_called()

    def test_minimal_or_failed_report_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            request_path, report_path, _, evidence_payloads = self._runtime_fixture(root)
            with patch.object(qa03.subprocess, "run", return_value=subprocess.CompletedProcess([], 0)):
                def runner(command, **kwargs):
                    self._write_fixture_output(report_path, {"task_id": "QA-03", "runtime_complete": True, "status": "PASS"}, evidence_payloads)
                    return subprocess.CompletedProcess(command, 0)
                qa03.subprocess.run.side_effect = runner
                with self.assertRaisesRegex(Qa03Error, "QA03_RUNTIME_REPORT_INCOMPLETE"):
                    run_live(request_path, {"QA_03_RUNTIME_COMMAND": "fixture"})

    def test_null_capacity_failed_case_or_missing_evidence_cannot_pass(self):
        for mutation, reason in (
            (lambda r: r["capacity"].update(duration_seconds=None), "QA03_RUNTIME_CAPACITY_INVALID"),
            (lambda r: r["cases"][0].update(status="FAIL"), "QA03_RUNTIME_CASE_INVALID"),
            (lambda r: r["cases"][0].update(evidence={"file": "missing.json", "sha256": SHA}), "QA03_RUNTIME_EVIDENCE_INVALID"),
            (lambda r: r["summary"].update(failed=6), "QA03_RUNTIME_SUMMARY_MISMATCH"),
            (lambda r: r["coverage"][0]["verified_archives"].pop(), "QA03_RUNTIME_ARCHIVE_COVERAGE_MISMATCH"),
            (lambda r: r["identity"].update(run_id="other-run"), "QA03_RUNTIME_REPORT_IDENTITY_MISMATCH"),
            (lambda r: r["cases"][0].update(operation_id="other-operation"), "QA03_RUNTIME_CASE_INVALID"),
            (lambda r: r["cases"][0].update(snapshots_after={"bootstrap": True, "refs": []}), "QA03_RUNTIME_SNAPSHOTS_INVALID"),
            (lambda r: r["cases"][0]["evidence"].update(sha256="0" * 64), "QA03_RUNTIME_EVIDENCE_CHECKSUM_MISMATCH"),
            (lambda r: r["capacity"].update(download_count=True), "QA03_RUNTIME_CAPACITY_INVALID"),
            (lambda r: r["cases"][5]["assertions"].update(scope_months=[]), "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda r: r["cases"][0]["assertions"].update(duplicate_count_delta=False), "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda r: r["cases"][1]["assertions"].update(download_count=False), "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda r: r["cases"][4]["assertions"]["outside_scope_after"].update(count=5), "QA03_RUNTIME_ASSERTION_FAILED"),
        ):
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                request_path, report_path, report, evidence_payloads = self._runtime_fixture(root)
                mutation(report)
                def runner(command, **kwargs):
                    self._write_fixture_output(report_path, report, evidence_payloads)
                    return subprocess.CompletedProcess(command, 0)
                with patch.object(qa03.subprocess, "run", side_effect=runner):
                    with self.assertRaisesRegex(Qa03Error, reason):
                        run_live(request_path, {"QA_03_RUNTIME_COMMAND": "fixture"})


if __name__ == "__main__":
    unittest.main()
