import importlib.util
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("range_recovery_test", ROOT / "scripts/dat03-range-recovery.py")
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)


class RangeRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.failed = "952973b42c8d4570b653ece99c79ed78"
        self.folder = self.root / ("a"*32); self.folder.mkdir()
        self.base = {"owner": "rosy179", "start_month": "2018-10", "end_month_exclusive": "2024-01", "run_id": "dat03-range-" + "a"*32}
        r.e.save(self.folder / "request.json", self.base)
        self.pin = r.e.sha(self.folder / "request.json")
        old = self.folder / "sessions" / self.failed; old.mkdir(parents=True)
        r.e.save(old / "execution.json", {"exit_code": 1, "timed_out": False, "wall_seconds": 2356.013})
        r.e.save(old / "intent.json", {"request_sha256": self.pin, "version": r.w.VERSION})
        (old / "stderr.private").write_text('Exception in thread "main" java.io.IOException: TRINO_QUERY_FAILED\n')
        (old / "stdout.private").write_text("event=dat03_catalog_phase month=2021-07 phase=verify\n")
        previous = None
        for month in r.w.months(self.base)[:33]:
            p = self.folder / "months" / month; p.mkdir(parents=True)
            r.e.save(p / "record.json", {"fixture": month})
            cp = {"month": month, "version": r.w.VERSION, "status": "PUBLISHED", "request_identity_sha256": r.e.digest(self.base),
                  "record_sha256": r.e.sha(p / "record.json"), "previous_checkpoint_sha256": previous}
            r.e.save(p / "checkpoint.json", cp); previous = r.e.sha(p / "checkpoint.json")
        p = self.folder / "months/2021-07"; p.mkdir()
        r.e.save(p / "context.json", {"resolver_request": {"target_months": ["2021-07", "2021-08", "2021-09"]}})
        for n in ("readiness", "bronze", "silver", "gold"): r.e.save(p / (n + ".json"), {"fixture": n})
        r.e.save(self.folder / "bronze-pins.json", {"fixture": True})

    def snapshot(self):
        with patch.object(r.w, "validate", return_value=self.base), patch.object(r.c, "quiet") as quiet:
            result = r.snapshot(self.folder, self.failed, self.pin)
            quiet.assert_called_once_with({"dag_id": r.w.DAG, "run_id": self.base["run_id"]})
            return result

    def test_trang_case_discovers_whole_committed_batch_not_one_month(self):
        before = sorted(str(p) for p in self.folder.rglob("*"))
        view = self.snapshot()
        self.assertEqual(33, view["initial_completed_months"])
        self.assertEqual(["2021-07", "2021-08", "2021-09"], view["target_months"])
        self.assertEqual(12, len(view["failed_files"]))
        self.assertEqual(before, sorted(str(p) for p in self.folder.rglob("*")))

    def test_wrong_request_pin_rejected(self):
        with patch.object(r.w, "validate", return_value=self.base), self.assertRaisesRegex(RuntimeError, "TRUSTED"):
            r.snapshot(self.folder, self.failed, "b"*64)

    def test_foreign_lease_or_active_runtime_never_released(self):
        with patch.object(r.w, "validate", return_value=self.base), patch.object(r.c, "quiet", side_effect=RuntimeError("FOREIGN_LEASE")):
            with self.assertRaisesRegex(RuntimeError, "FOREIGN_LEASE"): r.snapshot(self.folder, self.failed, self.pin)
        self.assertFalse((self.folder / "session-resolutions").exists())

    def test_timeout_unknown_and_boolean_exit_code_blocked(self):
        path = self.folder / "sessions" / self.failed / "execution.json"
        for x in ({"exit_code": 1, "timed_out": True}, {"exit_code": True, "timed_out": False}, {"exit_code": 0, "timed_out": False}):
            path.unlink()
            r.e.save(path, x)
            with self.assertRaisesRegex(RuntimeError, "KNOWN_TERMINAL"): self.snapshot()

    def test_published_failed_session_never_reclassified(self):
        r.e.save(self.folder / "sessions" / self.failed / "acceptance.json", {})
        with self.assertRaisesRegex(RuntimeError, "KNOWN_TERMINAL"): self.snapshot()

    def test_non_trino_error_or_publish_phase_blocked(self):
        path = self.folder / "sessions" / self.failed
        (path / "stderr.private").write_text("QUALITY_BLOCKER")
        with self.assertRaisesRegex(RuntimeError, "TRINO_FAILURE"): self.snapshot()
        (path / "stderr.private").write_text("java.io.IOException: TRINO_QUERY_FAILED")
        (path / "stdout.private").write_text("event=dat03_catalog_phase month=2021-07 phase=publish")
        with self.assertRaisesRegex(RuntimeError, "VERIFY_FAILURE"): self.snapshot()

    def test_noncontiguous_prefix_rejected(self):
        (self.folder / "months/2020-01/checkpoint.json").unlink()
        with self.assertRaisesRegex(RuntimeError, "NONCONTIGUOUS"): self.snapshot()

    def test_next_batch_cannot_skip_july(self):
        (self.folder / "months/2021-07/context.json").unlink()
        r.e.save(self.folder / "months/2021-07/context.json", {"resolver_request": {"target_months": ["2021-08"]}})
        with self.assertRaisesRegex(RuntimeError, "SCOPE_CHANGED"): self.snapshot()

    def test_second_unresolved_session_blocks_recovery(self):
        (self.folder / "sessions" / ("b"*32)).mkdir()
        with patch.object(r.w, "acceptance", side_effect=RuntimeError("OTHER_FAILURE")):
            with self.assertRaisesRegex(RuntimeError, "OTHER_FAILURE"): self.snapshot()

    def jar(self, name, files):
        p = self.root / name
        with zipfile.ZipFile(p, "w") as z:
            for k, v in files.items(): z.writestr(k, v)
        return p

    def test_overlay_preserves_every_original_entry_including_manifest(self):
        original = {"META-INF/MANIFEST.MF": b"old", "GoldPublisher.class": b"same", "TrinoSqlClient.class": b"same"}
        before = self.jar("before.jar", original)
        r.compatible(before, self.jar("good.jar", {**original, r.ENTRY: b"repair"}))
        for key in original:
            with self.assertRaisesRegex(RuntimeError, "ORIGINAL_CODE"):
                r.compatible(before, self.jar("bad-"+str(len(key))+".jar", {**original, key: b"changed", r.ENTRY: b"repair"}))
        with self.assertRaisesRegex(RuntimeError, "EXACT_CODE"):
            r.compatible(before, self.jar("extra.jar", {**original, r.ENTRY: b"repair", "Other.class": b"bad"}))

    def test_changed_failure_evidence_blocked(self):
        q = self.snapshot()
        (self.folder / "sessions" / self.failed / "stderr.private").write_text("edited")
        with self.assertRaisesRegex(RuntimeError, "EVIDENCE_CHANGED"): r.unchanged(self.folder, q)

    def test_resume_bridge_is_exactly_scoped_and_restores_original_function(self):
        failed = self.folder / "sessions" / self.failed
        other = self.folder / "sessions" / ("b"*32)
        accepted = {"completed_months": 36}
        def run_worker(*args):
            self.assertEqual(accepted, r.w.acceptance(self.folder, self.base, failed))
            r.w.acceptance(self.folder, self.base, other)
        with patch.object(r, "validate", return_value={"failed_session": self.failed}), \
                patch.object(r, "accepted_recovery", return_value=accepted), patch.object(r.w, "acceptance") as original, \
                patch.object(r.w, "worker", side_effect=run_worker):
            r.resume_worker(self.root, self.folder, self.base, 6000, 12)
            original.assert_called_once_with(self.folder, self.base, other)
            self.assertIs(r.w.acceptance, original)

    def test_unaccepted_recovery_cannot_resume(self):
        with patch.object(r, "validate", return_value={"failed_session": self.failed}), \
                patch.object(r, "accepted_recovery", side_effect=RuntimeError("NOT_ACCEPTED")), patch.object(r.w, "worker") as run:
            with self.assertRaisesRegex(RuntimeError, "NOT_ACCEPTED"):
                r.resume_worker(self.root, self.folder, self.base, 6000, 12)
            run.assert_not_called()

    def test_bad_failure_paths_and_symlinks_rejected(self):
        with self.assertRaises(RuntimeError): r.failure_names("../foreign", "2021-06", "2021-07")
        p = self.folder / "months/2021-07/gold.json"; p.unlink(); p.symlink_to(self.folder / "request.json")
        with self.assertRaisesRegex(RuntimeError, "SYMLINK"): self.snapshot()

    def measurement(self, attempt, q):
        return {"recovery_request_sha256": r.e.sha(attempt / "recovery-request.json"),
                "recovery_runner_sha256": q["runner_sha256"], "recovered_session": self.failed,
                "completed_months": 36, "required_months": 63,
                "current_snapshots": {t: 9007199254740997 for t in r.e.TABLES},
                "fresh_current_pin_checks": {t: {"actual": 9007199254740997, "expected": 9007199254740997} for t in r.e.TABLES}}

    def finalize_fixture(self):
        attempt = self.root / "attempt"; attempt.mkdir(); r.e.save(attempt / "recovery-request.json", {"fixture": True})
        q = {**self.snapshot(), "range_root": str(self.folder), "range_request_sha256": self.pin,
             "failed_session": self.failed, "recovery_session": "c"*32, "runner_sha256": "d"*64}
        session = self.folder / "sessions" / q["recovery_session"]; session.mkdir()
        r.e.save(session / "result.json", {"fixture": True})
        r.e.save(session / "execution.json", {"exit_code": 0, "timed_out": False})
        r.e.save(session / "validation.json", {"result_sha256": r.e.sha(session / "result.json"),
                "execution_sha256": r.e.sha(session / "execution.json"), "request_sha256": r.e.sha(attempt / "recovery-request.json")})
        return attempt, q, session

    def test_release_happens_only_after_validated_result_and_quiet_own_runtime(self):
        attempt, q, session = self.finalize_fixture()
        owner = {"dag_id": r.w.DAG, "run_id": self.base["run_id"]}
        with patch.object(r, "validate", return_value=q), patch.object(r.w, "validate", return_value=self.base), \
                patch.object(r, "compatible"), patch.object(r, "unchanged"), patch.object(r.c, "quiet") as quiet, \
                patch.object(r.w, "result", return_value=self.measurement(attempt, q)), patch.object(r, "accepted_recovery"), \
                patch.dict("sys.modules", {"source_run_guard": types.SimpleNamespace(lease=lambda action, held:
                           self.assertEqual(("release", owner), (action, held)) or {"status": "RELEASED", **owner})}):
            r.worker_locked(attempt, "finalize", 6000, 12)
            self.assertTrue(all(call.args == (owner,) for call in quiet.call_args_list))
        self.assertTrue((session / "acceptance.json").exists())
        self.assertFalse((self.folder / "sessions" / self.failed / "acceptance.json").exists())
        self.assertTrue((self.folder / "session-resolutions" / (self.failed + ".json")).exists())

    def test_bad_fresh_pin_proof_cannot_release_even_with_exit_zero(self):
        attempt, q, _ = self.finalize_fixture(); measured = self.measurement(attempt, q)
        measured["fresh_current_pin_checks"]["event_current"]["actual"] = True
        release = unittest.mock.Mock()
        with patch.object(r, "validate", return_value=q), patch.object(r.w, "validate", return_value=self.base), \
                patch.object(r, "compatible"), patch.object(r, "unchanged"), patch.object(r.c, "quiet"), \
                patch.object(r.w, "result", return_value=measured), \
                patch.dict("sys.modules", {"source_run_guard": types.SimpleNamespace(lease=release)}):
            with self.assertRaisesRegex(RuntimeError, "FRESH_PIN_CHECKS"):
                r.worker_locked(attempt, "finalize", 6000, 12)
            release.assert_not_called()

    def test_resolution_tamper_blocks_bridge(self):
        attempt, q, session = self.finalize_fixture()
        r.e.save(session / "acceptance.json", {"recovery_request_sha256": r.e.sha(attempt / "recovery-request.json")})
        (self.folder / "session-resolutions").mkdir()
        value = r.resolution_value(attempt, q, session); value["failed_session"] = "f"*32
        r.e.save(self.folder / "session-resolutions" / (self.failed + ".json"), value)
        with patch.object(r, "validate", return_value=q), patch.object(r.w, "acceptance", return_value=self.measurement(attempt, q)):
            with self.assertRaisesRegex(RuntimeError, "RESOLUTION_CHANGED"):
                r.accepted_recovery(attempt, self.folder, self.base)

    def test_remote_inspection_source_is_executable_without_repo_imports(self):
        # The code shipped through docker exec must not accidentally rely on host-only globals.
        import inspect
        code = inspect.getsource(r.failure_names) + "\n" + inspect.getsource(r.snapshot)
        namespace = {"w": r.w, "c": r.c, "e": r.e, "re": r.re, "ID": r.ID}
        exec(code, namespace)
        with patch.object(r.w, "validate", return_value=self.base), patch.object(r.c, "quiet"):
            self.assertEqual(33, namespace["snapshot"](self.folder, self.failed, self.pin)["initial_completed_months"])

    def prepared(self):
        view = self.snapshot()
        folder = self.root / "staging/dat03-range" / self.folder.name
        folder.parent.mkdir(parents=True); self.folder.rename(folder); self.folder = folder
        for source, dest in (("dat03-range-worker.py", "controller.py"), ("research-catalog.py", "research-catalog.py"), ("research-e2e.py", "research-e2e.py")):
            (folder / dest).write_bytes((ROOT / "scripts" / source).read_bytes())
        self.jar("staging/dat03-range/" + folder.name + "/runner.jar", {"original.class": b"original", "META-INF/MANIFEST.MF": b"frozen"})
        compiled = self.jar("build.jar", {r.ENTRY: b"new-class-fixture"})
        with patch.object(r.w, "validate", return_value=self.base), patch.object(r, "inspect_runtime", return_value=view), \
                patch.object(r.shutil, "disk_usage", return_value=types.SimpleNamespace(free=5*1024**3)):
            return r.prepare(self.root, folder, self.failed, self.pin, compiled)

    def test_prepare_freezes_original_controller_and_adds_only_reviewed_entry(self):
        attempt = self.prepared(); q = r.validate(attempt)
        self.assertEqual(self.pin, q["range_request_sha256"])
        self.assertEqual((self.folder / "controller.py").read_bytes(), (attempt / "range-worker.py").read_bytes())
        r.compatible(self.folder / "runner.jar", attempt / "runner.jar")
        self.assertFalse((self.folder / "session-resolutions").exists())
        (attempt / "range-worker.py").write_text("edited-controller")
        with self.assertRaisesRegex(RuntimeError, "CODE_CHANGED"): r.validate(attempt)

    def test_host_service_change_prevents_finalize_and_release(self):
        attempt = self.prepared()
        services = unittest.mock.Mock(side_effect=[{"container": "before"}, {"container": "changed"}])
        with patch.object(r.c, "module", return_value=types.SimpleNamespace(services=services)), \
                patch.object(r.shutil, "disk_usage", return_value=types.SimpleNamespace(free=5*1024**3)), \
                patch.object(r.subprocess, "run") as command:
            with self.assertRaisesRegex(RuntimeError, "SERVICES_CHANGED_LEASE_RETAINED"):
                r.run(self.root, attempt, "run")
            self.assertEqual(1, command.call_count)
            self.assertNotIn("finalize", command.call_args.args[0])


if __name__ == "__main__": unittest.main()
