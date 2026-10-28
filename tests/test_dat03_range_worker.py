import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("range_worker_test", ROOT / "scripts/dat03-range-worker.py")
w = importlib.util.module_from_spec(spec)
spec.loader.exec_module(w)


class RangeWorkerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def inputs(self):
        root = self.root / "inputs-release"
        root.mkdir(); (root / "inputs").mkdir()
        sources = []
        captures = []
        for name in w.c.NAMES:
            meta_name, raw_name = w.c.source_files(name)
            (root/raw_name).write_bytes(b"test-public-source")
            meta = {"sha256": w.e.sha(root/raw_name), "bytes": (root/raw_name).stat().st_size,
                    "retrieved_at_utc": "2026-10-24T00:00:00Z"}
            w.e.save(root/meta_name, meta)
            p = {"name": name, "raw_sha256": meta["sha256"], "metadata_sha256": w.e.sha(root/meta_name), "bytes": meta["bytes"]}
            sources.append(p)
            if name.startswith("usgs-"):
                captures.append({"month": name[5:], "raw_sha256": p["raw_sha256"], "capture_sha256": p["metadata_sha256"], "check": {"feature_count": 0}})
        for name in ("baseline.csv", "boundary.csv"): (root/name).write_bytes(b"fixture\n")
        w.e.save(root / "plan.json", {"batches": [{"month": m} for m in w.c.module_months()]})
        w.e.save(root / "usgs-summary.json", {"captures": captures})
        m = {"version": "dat03-range-inputs-v1", "source_cutoff_utc": "2026-10-25T06:13:04.747Z",
             "catalog_request_sha256": "a"*64, "sources": sources, "research_ready": False,
             "files": {n: {"sha256": w.e.sha(root/n), "bytes": (root/n).stat().st_size} for n in w.payload_names()}}
        w.e.save(root / "manifest.json", m)
        return root, w.e.sha(root / "manifest.json")

    def test_exact_payload_and_runtime_transfer_count(self):
        self.assertEqual(78, len(w.payload_names()))
        self.assertEqual(84, len(w.frozen_files()))
        self.assertFalse(any("private" in n or "capacity" in n or ".env" in n for n in w.frozen_files()))

    def test_three_disjoint_ranges(self):
        seen = set()
        for owner, (start, end) in w.ASSIGNMENTS.items():
            ms = w.months({"owner": owner, "start_month": start, "end_month_exclusive": end})
            self.assertEqual(63 if owner == "rosy179" else 62, len(ms))
            self.assertFalse(seen.intersection(ms)); seen.update(ms)
        self.assertEqual(187, len(seen))

    def test_reject_assignment_extension(self):
        with self.assertRaisesRegex(RuntimeError, "ASSIGNMENT"):
            w.months({"owner": "ThanhTris", "start_month": "2013-08", "end_month_exclusive": "2018-11"})

    def test_verify_37_frozen_sources(self):
        root, pin = self.inputs()
        self.assertEqual(37, len(w.verify_inputs(root, pin)["sources"]))

    def test_external_pin_required(self):
        root, _ = self.inputs()
        with self.assertRaisesRegex(RuntimeError, "TRUSTED"):
            w.verify_inputs(root, "a"*64)

    def test_mutated_raw_rejected(self):
        root, pin = self.inputs()
        (root / "inputs/usgs-2023-12.geojson").write_bytes(b"latest-is-not-frozen")
        with self.assertRaisesRegex(RuntimeError, "INPUT_CHANGED"):
            w.verify_inputs(root, pin)

    def test_missing_boundary_rejected(self):
        root, pin = self.inputs(); (root / "inputs/h2024.zip").unlink()
        with self.assertRaisesRegex(RuntimeError, "INPUT_CHANGED"):
            w.verify_inputs(root, pin)

    def test_duplicate_manifest_key_rejected(self):
        root, _ = self.inputs(); f = root / "manifest.json"
        f.write_text('{"version":1,"version":2}')
        with self.assertRaisesRegex(RuntimeError, "DUPLICATE_JSON"):
            w.verify_inputs(root, w.e.sha(f))

    def test_symlink_rejected(self):
        root, pin = self.inputs(); f = root / "inputs/h2024.zip"
        f.unlink(); f.symlink_to(root / "inputs/h2023.zip")
        with self.assertRaisesRegex(RuntimeError, "SYMLINK"):
            w.verify_inputs(root, pin)

    def test_prepare_pins_all_code_and_assignment(self):
        import zipfile
        root, pin = self.inputs(); jar = self.root / "runner.jar"
        with zipfile.ZipFile(jar, "w") as z: z.writestr(w.CHECK.replace(".", "/") + ".class", b"fixture-not-runtime")
        with patch.object(w.subprocess, "run") as run:
            run.return_value.stdout = "RANGE_ORIGINAL_37_SOURCES_VERIFIED; no Bronze/Silver/Gold writes; research_ready=false"
            folder = w.prepare(self.root, root, pin, "ThanhTris", jar)
            self.assertIn("ie212.earthquake.spark.usgs.ResearchRangeInputCheck", run.call_args.args[0])
        r = w.validate(folder)
        self.assertEqual("2013-08", r["start_month"])
        self.assertNotIn("trino_capacity", r)  # Never import another machine's capacity acceptance.
        (folder / "research-catalog.py").write_text("changed")
        with self.assertRaisesRegex(RuntimeError, "CODE_OR_CONFIG"):
            w.validate(folder)

    def test_foreign_lease_blocks_before_session_or_process(self):
        with patch.object(w, "validate", return_value={}), patch.object(w.c, "quiet", side_effect=RuntimeError("FOREIGN_LEASE")), patch.object(w.subprocess, "Popen") as process:
            with self.assertRaisesRegex(RuntimeError, "FOREIGN_LEASE"): w.execute(self.root, 6000, 1)
            process.assert_not_called()
        self.assertFalse((self.root / "sessions").exists())

    def test_previous_failed_session_cannot_auto_resume(self):
        (self.root / "sessions" / ("a"*32)).mkdir(parents=True)
        with patch.object(w, "validate", return_value={}), patch.object(w.c, "quiet"), patch.object(w.subprocess, "Popen") as process:
            with self.assertRaises(RuntimeError): w.execute(self.root, 6000, 1)
            process.assert_not_called()

    def test_host_budget_rejects_more_than_12(self):
        folder = self.root / "staging/dat03-range" / ("a"*32)
        with patch.object(w, "validate") as validate:
            with self.assertRaisesRegex(RuntimeError, "BUDGET"): w.run(self.root, folder, 6000, 13, False)
            validate.assert_not_called()

    def test_first_session_cannot_roll_out_12_without_local_acceptance(self):
        with patch.object(w, "validate", return_value={}), patch.object(w.c, "quiet"), patch.object(w.subprocess, "Popen") as process:
            with self.assertRaisesRegex(RuntimeError, "FIRST_SESSION_ONE_MONTH"):
                w.execute(self.root, 6000, 12)
            process.assert_not_called()

    def lifecycle(self, exit_code=0, result_error=None):
        calls = []
        def lease(action, owner):
            calls.append(action)
            return {"status": "RELEASED" if action == "release" else "ACQUIRED", **owner}
        (self.root / "request.json").write_text('{}')
        request = {"run_id": "dat03-range-" + "a"*32, "owner": "ThanhTris"}
        def checked_result(folder, r, session):
            if result_error: raise RuntimeError(result_error)
            (session / "result.json").write_text('{}')
            return {"completed_months": 1, "required_months": 62}
        with patch.object(w, "validate", return_value=request), patch.object(w.c, "quiet"), \
                patch.object(Path, "is_file", return_value=True), patch.object(w.e, "sha", return_value="a"*64), \
                patch.object(w, "result", side_effect=checked_result), \
                patch.object(w.shutil, "disk_usage", return_value=SimpleNamespace(free=8*1024**3)), \
                patch.dict(w.sys.modules, {"source_run_guard": SimpleNamespace(lease=lease)}), \
                patch.object(w.subprocess, "Popen") as process:
            process.return_value.wait.return_value = exit_code
            if exit_code or result_error:
                with self.assertRaises(RuntimeError): w.execute(self.root, 6000, 1)
            else: w.execute(self.root, 6000, 1)
            self.assertIn(w.CHECK, process.call_args.args[0])
        return calls

    def test_success_releases_only_own_lease_after_acceptance(self):
        self.assertEqual(["acquire", "release"], self.lifecycle())
        self.assertEqual(1, len(list((self.root / "sessions").glob('*/acceptance.json'))))

    def test_java_failure_keeps_lease_and_execution_evidence(self):
        self.assertEqual(["acquire"], self.lifecycle(exit_code=1))
        self.assertEqual(1, len(list((self.root / "sessions").glob('*/execution.json'))))
        self.assertEqual([], list((self.root / "sessions").glob('*/acceptance.json')))

    def test_pending_or_unverified_result_keeps_lease(self):
        self.assertEqual(["acquire"], self.lifecycle(result_error="PENDING_MONTH"))
        self.assertEqual([], list((self.root / "sessions").glob('*/acceptance.json')))


if __name__ == "__main__": unittest.main()
