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

    def test_inventory_pin_matches_git_blob_and_is_lf_on_checkout(self):
        expected = "61dc2c73f1bb9cee48fba851cbb941cc62c893201c36db583cdd55ae19f8199a"
        raw = INVENTORY.read_bytes()
        self.assertEqual(expected, hashlib.sha256(raw).hexdigest())
        self.assertNotIn(b"\r\n", raw)
        blob = subprocess.check_output(["git", "show", "HEAD:config/jma/hypocenter_archives_v1.csv"], cwd=Path(__file__).parents[2])
        self.assertEqual(expected, hashlib.sha256(blob).hexdigest())
        self.assertEqual(blob, raw)
        for autocrlf in ("true", "false"):
            with self.subTest(core_autocrlf=autocrlf), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                repo = root / "repo"
                repo.mkdir()
                shutil.copyfile(Path(__file__).parents[2] / ".gitattributes", repo / ".gitattributes")
                inventory = repo / "config/jma/hypocenter_archives_v1.csv"
                inventory.parent.mkdir(parents=True)
                inventory.write_bytes(blob)
                subprocess.run(["git", "init", "--quiet", str(repo)], check=True)
                subprocess.run(["git", "-C", str(repo), "add", ".gitattributes", "config/jma/hypocenter_archives_v1.csv"], check=True)
                subprocess.run(["git", "-C", str(repo), "-c", "user.name=QA Fixture", "-c",
                                "user.email=qa-fixture@example.invalid", "commit", "--quiet", "-m", "fixture"], check=True)
                output = root / "checkout"
                output.mkdir()
                subprocess.run(["git", "-C", str(repo), "-c", f"core.autocrlf={autocrlf}",
                                "checkout-index", "--force", f"--prefix={output}{os.sep}",
                                "config/jma/hypocenter_archives_v1.csv"], check=True)
                checked_out = (output / "config/jma/hypocenter_archives_v1.csv").read_bytes()
                self.assertEqual(expected, hashlib.sha256(checked_out).hexdigest())
                self.assertNotIn(b"\r\n", checked_out)

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
        report = {"task_id": "QA-03", "report_version": "qa-03-report-v3", "status": "PASS",
                  "runtime_complete": True, "identity": identity, "verified_input_refs": request["input_refs"],
                  "scope": matrix["_selected_scope"], "coverage": [], "cases": [],
                  "capacity": {"duration_seconds": 12.5, "peak_memory_mb": 42.0, "download_count": 1,
                               "download_reused": 3, "verified_archive_reused": 3, "resource_failures": 1},
                  "summary": {"scope_count": 4, "case_count": 6, "passed": 6, "failed": 0, "pending_runtime": 0},
                  "snapshots": {"bootstrap": False, "refs": []}, "gold_publications": []}
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
            "changed_release": {"month": "1997-09", "archive_name": "h199701.zip",
                "old_catalog_release": "old", "new_catalog_release": "release-h199701.zip",
                "new_manifest_sha256": next(ref["sha256"] for ref in input_refs if ref["archive_name"] == "h199701.zip"),
                "old_snapshot": {"table": "silver.events", "snapshot_id": 10, "catalog_release": "old"},
                "new_snapshot": {"table": "silver.events", "snapshot_id": 11, "catalog_release": "release-h199701.zip"},
                "old_snapshot_readback": {"table": "silver.events", "snapshot_id": 10, "catalog_release": "old", "readable": True}},
            "same_release_different_bytes": {"rejected": True, "downstream_blocked": True, "data_unchanged": True, "old_sha256": SHA, "new_sha256": "b" * 64},
            "retention_outside_scope": {"outside_scope_before": {"table": "gold.event_current", "snapshot_id": 20, "partition": "2001-01", "count": 4, "content_sha256": "c" * 64},
                "outside_scope_after": {"table": "gold.event_current", "snapshot_id": 21, "partition": "2001-01", "count": 4, "content_sha256": "c" * 64},
                "snapshot_readback": {"table": "gold.event_current", "snapshot_id": 20, "partition": "2001-01", "count": 4, "content_sha256": "c" * 64, "readable": True}},
            "resource_guard": {"batch_months": ["1997-09"], "guard_triggered": True, "recovery_passed": True, "scope_months": ["1997-09"]},
        }
        evidence_payloads = {}
        for case in matrix["cases"]:
            case_id = case["case_id"]
            assertions = assertions_by_case[case_id]
            if case_id == "changed_release":
                before = {"bootstrap": False, "refs": [assertions["old_snapshot"]]}
                after = {"bootstrap": False, "refs": [assertions["old_snapshot"], assertions["new_snapshot"]]}
            elif case_id == "retention_outside_scope":
                before = {"bootstrap": False, "refs": [assertions["outside_scope_before"]]}
                after = {"bootstrap": False, "refs": [assertions["outside_scope_before"], assertions["outside_scope_after"]]}
            else:
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
        for month_index, month in enumerate(sorted(item["month"] for item in matrix["_selected_scope"])):
            gold_run = "qa03-gold-" + month.replace("-", "")
            namespace = "iceberg.qa_03_jma_backfill"
            table_names = ("event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band")
            tables = {name: {"table": namespace + "." + name, "snapshot_id": 100 + month_index * 10 + index,
                             "count": 5 + index, "schema_json": "{}"}
                      for index, name in enumerate(table_names)}
            commit = {"gold_run_id": gold_run, "identity_sha256": hashlib.sha256(gold_run.encode()).hexdigest(),
                      "namespace": namespace, "status": "COMMITTED", "published": False,
                      "scope": {"context": {"run_id": gold_run}, "affected_months": [month]}, "tables": tables}
            commit_bytes = (json.dumps(commit, sort_keys=True, separators=(",", ":")) + "\n").encode()
            bundle_sha = hashlib.sha256(commit_bytes).hexdigest()
            checks = {}
            def add_check(name, expected):
                checks[name] = {"sql": "SELECT fixture", "expected": expected, "actual": expected}
            for name in table_names:
                add_check("count:" + name, tables[name]["count"])
                add_check("identity:" + name, 1)
                add_check("current:" + name, tables[name]["snapshot_id"])
                add_check("unique:" + name, 0)
            for name in ("required_fields", "bridge_lineage", "source_counts", "primary_provenance", "utc_jst",
                         "dimensions", "aggregate_count", "changed_run_scope", "outside:event_current", "outside:event_source_bridge"):
                add_check(name, 0)
            add_check("serving_count", 5)
            add_check("stable_serving_count", 5)
            publication = {"task": "GLD-04", "engine": "TRINO", "verify_status": "PASSED", "published": True,
                           "namespace": namespace, "gold_run_id": gold_run, "identity_sha256": commit["identity_sha256"],
                           "bundle_sha256": bundle_sha, "publication_id": "pub_" + bundle_sha,
                           "snapshot_bundle": tables, "scope": commit["scope"], "checks": checks}
            publication_bytes = (json.dumps(publication, sort_keys=True, separators=(",", ":")) + "\n").encode()
            commit_file, publication_file = f"gold/{month}/commit.json", f"gold/{month}/publication.json"
            evidence_payloads[commit_file] = commit_bytes
            evidence_payloads[publication_file] = publication_bytes
            report["gold_publications"].append({"month": month, "run_id": request["run_id"],
                "operation_id": request["operation_id"], "namespace": request["namespace"],
                "commit": {"file": commit_file, "sha256": hashlib.sha256(commit_bytes).hexdigest()},
                "publication": {"file": publication_file, "sha256": hashlib.sha256(publication_bytes).hexdigest()}})
            report["snapshots"]["refs"].extend({"table": pin["table"], "snapshot_id": pin["snapshot_id"], "month": month}
                                                for pin in tables.values())
        return request_path, report_path, report, evidence_payloads

    def _write_fixture_output(self, report_path, report, evidence_payloads):
        evidence_dir = report_path.parent / (report_path.name + ".evidence")
        evidence_dir.mkdir()
        for filename, content in evidence_payloads.items():
            if filename.startswith("gold/"):
                target = report_path.parent / filename
                target.parent.mkdir(parents=True, exist_ok=True)
            else:
                target = evidence_dir / filename
            target.write_bytes(content)
        report_path.write_text(json.dumps(report), encoding="utf-8")

    def _assert_report_rejected(self, request_path, report_path, report, evidence_payloads, reason):
        self._write_fixture_output(report_path, report, evidence_payloads)
        request = json.loads(request_path.read_text(encoding="utf-8"))
        matrix = load_matrix(request["matrix_path"], request["inventory_path"])
        report = json.loads(report_path.read_text(encoding="utf-8"))
        with self.assertRaisesRegex(Qa03Error, reason):
            qa03._validate_runtime_report(report, request, matrix, report_path)

    def _mutate_publication(self, report, payloads, month, change):
        receipt = next(item for item in report["gold_publications"] if item["month"] == month)
        filename = receipt["publication"]["file"]
        publication = json.loads(payloads[filename])
        change(publication)
        raw = (json.dumps(publication, sort_keys=True, separators=(",", ":")) + "\n").encode()
        payloads[filename] = raw
        receipt["publication"]["sha256"] = hashlib.sha256(raw).hexdigest()

    def _mutate_commit(self, report, payloads, month, change):
        receipt = next(item for item in report["gold_publications"] if item["month"] == month)
        filename = receipt["commit"]["file"]
        commit = json.loads(payloads[filename])
        change(commit)
        raw = (json.dumps(commit, sort_keys=True, separators=(",", ":")) + "\n").encode()
        payloads[filename] = raw
        receipt["commit"]["sha256"] = hashlib.sha256(raw).hexdigest()

    def _refresh_case_evidence(self, report, payloads, case):
        filename = case["evidence"]["file"]
        evidence = json.loads(payloads[filename])
        evidence["assertions"] = case["assertions"]
        evidence["snapshots_before"] = case["snapshots_before"]
        evidence["snapshots_after"] = case["snapshots_after"]
        raw = (json.dumps(evidence, sort_keys=True, separators=(",", ":")) + "\n").encode()
        payloads[filename] = raw
        case["evidence"]["sha256"] = hashlib.sha256(raw).hexdigest()

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
                "[(target.parent/name if name.startswith('gold/') else folder/name).parent.mkdir(parents=True,exist_ok=True) for name in evidence]\n"
                "[(target.parent/name if name.startswith('gold/') else folder/name).write_bytes(body.encode('utf-8')) for name,body in evidence.items()]\n"
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

    def test_legacy_report_v2_cannot_claim_runtime_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            request_path, report_path, report, payloads = self._runtime_fixture(Path(directory))
            report["report_version"] = "qa-03-report-v2"
            def runner(command, **kwargs):
                self._write_fixture_output(report_path, report, payloads)
                return subprocess.CompletedProcess(command, 0)
            with patch.object(qa03.subprocess, "run", side_effect=runner):
                with self.assertRaisesRegex(Qa03Error, "QA03_RUNTIME_REPORT_INCOMPLETE"):
                    run_live(request_path, {"QA_03_RUNTIME_COMMAND": "fixture"})

    def test_changed_release_snapshots_must_match_receipts_and_pinned_release(self):
        mutations = (
            (lambda case: case["assertions"]["new_snapshot"].update(snapshot_id=999), "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda case: case["assertions"]["new_snapshot"].update(table="silver.other"), "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda case: case["assertions"]["new_snapshot"].update(catalog_release="other"), "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda case: case["assertions"].update(new_manifest_sha256="0" * 64), "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda case: case["snapshots_before"]["refs"][0].update(snapshot_id=11), "QA03_RUNTIME_ASSERTION_FAILED"),
        )
        for index, (change, reason) in enumerate(mutations):
            with self.subTest(mutation=index, reason=reason), tempfile.TemporaryDirectory() as directory:
                request_path, report_path, report, payloads = self._runtime_fixture(Path(directory))
                report = json.loads(json.dumps(report))
                case = next(item for item in report["cases"] if item["case_id"] == "changed_release")
                change(case)
                self._refresh_case_evidence(report, payloads, case)
                self._assert_report_rejected(request_path, report_path, report, payloads, reason)

    def test_outside_scope_partition_and_snapshot_receipts_are_verified(self):
        mutations = (
            (lambda case: case["assertions"]["outside_scope_before"].update(partition="1997-09"),
             "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda case: case["assertions"]["outside_scope_before"].update(partition="2001-13"),
             "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda case: case["assertions"]["outside_scope_after"].update(snapshot_id=999),
             "QA03_RUNTIME_ASSERTION_FAILED"),
            (lambda case: case["assertions"]["snapshot_readback"].update(content_sha256="d" * 64),
             "QA03_RUNTIME_ASSERTION_FAILED"),
        )
        for index, (change, reason) in enumerate(mutations):
            with self.subTest(mutation=index, reason=reason), tempfile.TemporaryDirectory() as directory:
                request_path, report_path, report, payloads = self._runtime_fixture(Path(directory))
                report = json.loads(json.dumps(report))
                case = next(item for item in report["cases"] if item["case_id"] == "retention_outside_scope")
                change(case)
                self._refresh_case_evidence(report, payloads, case)
                self._assert_report_rejected(request_path, report_path, report, payloads, reason)

    def test_runtime_pass_requires_four_published_gold_bundles_and_trino_checks(self):
        mutations = (
            (lambda report, payloads: report.update(gold_publications=[]),
             "QA03_RUNTIME_GOLD_PUBLICATION_INCOMPLETE"),
            (lambda report, payloads: report["gold_publications"][0]["publication"].update(sha256="0" * 64),
             "QA03_RUNTIME_GOLD_ARTIFACT_CHECKSUM_MISMATCH"),
            (lambda report, payloads: report["gold_publications"][1].update(month="1997-09"),
             "QA03_RUNTIME_GOLD_PUBLICATION_IDENTITY_MISMATCH"),
            (lambda report, payloads: report["gold_publications"][0].update(namespace="prod.gold"),
             "QA03_RUNTIME_GOLD_PUBLICATION_IDENTITY_MISMATCH"),
            (lambda report, payloads: report["snapshots"]["refs"][0].update(snapshot_id=999),
             "QA03_RUNTIME_GOLD_SNAPSHOT_BUNDLE_MISMATCH"),
            (lambda report, payloads: self._mutate_publication(report, payloads, "1997-09",
                lambda publication: publication.update(published=False)), "QA03_RUNTIME_GOLD_PUBLICATION_INVALID"),
            (lambda report, payloads: self._mutate_commit(report, payloads, "1997-09",
                lambda commit: commit["tables"].pop("dim_date")), "QA03_RUNTIME_GOLD_COMMIT_INVALID"),
            (lambda report, payloads: self._mutate_publication(report, payloads, "1997-09",
                lambda publication: publication["snapshot_bundle"].pop("dim_date")), "QA03_RUNTIME_GOLD_PUBLICATION_INVALID"),
            (lambda report, payloads: self._mutate_publication(report, payloads, "1997-09",
                lambda publication: publication["checks"]["required_fields"].update(actual=1)),
             "QA03_RUNTIME_GOLD_CHECK_FAILED"),
        )
        for change, reason in mutations:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as directory:
                request_path, report_path, report, payloads = self._runtime_fixture(Path(directory))
                change(report, payloads)
                self._assert_report_rejected(request_path, report_path, report, payloads, reason)

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
