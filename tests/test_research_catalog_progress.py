import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("catalog_progress_test", Path(__file__).resolve().parents[1] / "scripts/research-catalog-progress.py")
p = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(p)


class CatalogProgressTest(unittest.TestCase):
    def test_only_exact_accepted_month_thresholds_are_milestones(self):
        for count, expected in ((0, []), (27, []), (71, []), (72, [25]), (143, [25]),
                                (144, [25, 50]), (216, [25, 50, 75]), (288, [25, 50, 75, 100])):
            self.assertEqual(expected, p.milestones(count))
        for count in (True, 72.0, -1, 289):
            with self.assertRaisesRegex(RuntimeError, "TYPED_COUNT"):
                p.milestones(count)

    def fixture(self, root):
        catalog, runtime = root / "catalog", root / "runtime"
        catalog.mkdir(); runtime.mkdir(); (runtime / "months").mkdir()
        request = {"fixture": True, "run_id": "dat03-catalog-" + "a" * 32}
        p.c.e.save(catalog / "request.json", request); p.c.e.save(runtime / "request.json", request)
        session = runtime / "sessions" / ("b" * 32); session.mkdir(parents=True)
        execution = {"exit_code": 0, "timed_out": False, "wall_seconds": 1200, "child_peak_rss_kib": 1000}
        result = {"completed_months": 0, "completed_in_session": 0, "reproduction_months": 0,
                  "extension_months": 0, "current_snapshots": {table: 0 for table in p.c.e.TABLES}}
        p.c.e.save(session / "execution.json", execution); p.c.e.save(session / "result.json", result)
        acceptance = {"version": p.c.VERSION, "status": "CHECKPOINT_ACCEPTED",
                      "request_sha256": p.c.e.sha(catalog / "request.json"), "result_sha256": p.c.e.sha(session / "result.json"),
                      "execution_sha256": p.c.e.sha(session / "execution.json"),
                      "lease_release": {"status": "RELEASED", "dag_id": p.c.DAG, "run_id": request["run_id"]},
                      "official_exported": False, "research_ready": False}
        p.c.e.save(session / "acceptance.json", acceptance)
        return catalog, runtime, session, request, result

    def test_empty_checkpoint_report_never_claims_dataset_done_or_writes_runtime(self):
        with tempfile.TemporaryDirectory() as temp:
            catalog, runtime, session, request, result = self.fixture(Path(temp).resolve())
            with patch.object(p.c, "validate", return_value=request), patch.object(p.c, "session_result", return_value=result), \
                    patch.object(p.c.subprocess, "run", side_effect=AssertionError("no Docker")), patch.object(p.c.e, "save", side_effect=AssertionError("read-only")):
                report = p.report(catalog, runtime, session.name)
            self.assertEqual([], report["reached_milestones"])
            self.assertEqual("CATALOG_ONLY_NOT_DAT03_COMPLETION", report["scope"])
            self.assertFalse(report["research_ready"]); self.assertFalse(report["official_exported"])

    def test_changed_execution_or_missing_acceptance_cannot_report_success(self):
        with tempfile.TemporaryDirectory() as temp:
            catalog, runtime, session, request, result = self.fixture(Path(temp).resolve())
            with patch.object(p.c, "validate", return_value=request), patch.object(p.c, "session_result", return_value=result):
                for key, value in (("exit_code", True), ("timed_out", True), ("wall_seconds", 8000)):
                    execution = p.c.load(session / "execution.json")
                    execution[key] = value
                    # Intentional fixture tampering, never pipeline evidence.
                    (session / "execution.json").write_text(__import__("json").dumps(execution))
                    with self.assertRaisesRegex(RuntimeError, "ACCEPTANCE_CHANGED"):
                        p.report(catalog, runtime, session.name)

    def test_session_traversal_is_rejected_before_reading_inputs(self):
        with patch.object(p.c, "validate", side_effect=AssertionError("no file reads")):
            for name in ("../other", "A" * 32, "a" * 31, True):
                with self.assertRaisesRegex(RuntimeError, "EXACT_SESSION"):
                    p.report(Path("/unused"), Path("/unused"), name)


if __name__ == "__main__":
    unittest.main()
