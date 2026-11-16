"""Offline unit tests for DEMO-01 demo package manifest and preflight verifier."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("demo_preflight", ROOT / "scripts" / "demo-preflight.py")
preflight = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(preflight)


class DemoPreflightTests(unittest.TestCase):
    def test_repo_manifest_passes_offline_with_pending_gates(self):
        report = preflight.run_preflight()
        self.assertEqual(report["task_id"], "DEMO-01")
        self.assertEqual(report["overall_status"], "PASS_WITH_PENDING_GATES")
        self.assertTrue(report["demo_package_prepared"])
        self.assertFalse(report["official_release_ready"])
        self.assertFalse(report["depends_on_power_bi"])
        self.assertFalse(report["stage_40_year_rerun_required"])
        self.assertEqual(report["verified_evidence_count"], 10)
        self.assertEqual(len(report["fallback_modes_verified"]), 4)
        self.assertGreaterEqual(len(report["pending_official_gates"]), 1)

    def test_optional_local_receipt_missing_reports_absent_without_failing(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing-receipt.json"
            report = preflight.run_preflight(local_receipt_path=missing)
            self.assertEqual(report["local_research_receipt"]["status"], "ABSENT")
            self.assertTrue(report["local_research_receipt"]["checked"])

            valid_receipt = Path(tmp) / "ThanhTris-research-receipt.json"
            valid_receipt.write_text(
                json.dumps(
                    {
                        "status": "TEAM_RESEARCH_VERIFIED",
                        "recipient": "ThanhTris",
                        "all_shards_verified": True,
                        "handoff_sha256": "6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297",
                    }
                ),
                encoding="utf-8",
            )
            present_report = preflight.run_preflight(local_receipt_path=valid_receipt)
            self.assertEqual(present_report["local_research_receipt"]["status"], "TEAM_RESEARCH_VERIFIED")
            self.assertEqual(present_report["local_research_receipt"]["recipient"], "ThanhTris")
            self.assertTrue(present_report["local_research_receipt"]["all_shards_verified"])

    def _run_with_mutated_manifest(self, mutate_fn):
        base = json.loads(preflight.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
        mutated = copy.deepcopy(base)
        mutate_fn(mutated)
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "manifest.json"
            manifest_path.write_text(json.dumps(mutated, ensure_ascii=False), encoding="utf-8")
            return preflight.run_preflight(manifest_path=manifest_path, root=ROOT)

    def test_missing_required_file_fails_closed(self):
        with self.assertRaisesRegex(FileNotFoundError, "DEMO_MISSING_FILE"):
            self._run_with_mutated_manifest(
                lambda m: m["required_repo_files"].append("docs/does_not_exist_demo.md")
            )

    def test_path_traversal_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PATH_ESCAPES_ROOT"):
            self._run_with_mutated_manifest(
                lambda m: m["required_repo_files"].append("../outside.txt")
            )

    def test_mismatched_run_id_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:QA-01.run_id"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["daily_operational_v2"].__setitem__("run_id", "wrong-run")
            )

    def test_mismatched_snapshot_id_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:QA-01.snapshot.event_current"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["daily_operational_v2"]["snapshot_ids"].__setitem__(
                    "event_current", "999"
                )
            )

    def test_official_ready_with_pending_gates_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_CANNOT_BE_OFFICIAL_READY_WITH_PENDING_GATES"):
            self._run_with_mutated_manifest(lambda m: m.__setitem__("official_release_ready", True))

    def test_missing_scientific_boundary_statement_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_BOUNDARY_STATEMENT_INCOMPLETE"):
            self._run_with_mutated_manifest(
                lambda m: m["scientific_boundary"].__setitem__("statement", "Du doan dong dat thanh cong")
            )

    def test_secret_pattern_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_SECRET_PATTERN_DETECTED"):
            self._run_with_mutated_manifest(
                lambda m: m.__setitem__("leaked", "MINIO_ROOT_PASSWORD=supersecretvalue")
            )


if __name__ == "__main__":
    unittest.main()
