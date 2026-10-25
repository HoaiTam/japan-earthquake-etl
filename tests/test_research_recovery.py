"""Synthetic block-1 guards; never execute Docker, sources, SQL or real ETL."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("research_recovery", ROOT / "scripts/research-recovery.py")
recovery = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(recovery)
e2e = recovery.e2e


class ResearchRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.request = {"run_id": "dat03-" + "a" * 32, "namespace": "iceberg.dat03_e2e_" + "a" * 32, "runner_sha256": "a" * 64}
        self.pins = {"preflight_runner_sha256": "b" * 64, "request_sha256": "c" * 64, "execution_sha256": "d" * 64,
                     "incomplete_snapshots": {t: 123 for t in e2e.TABLES}}

    def plan(self):
        out = {"version": recovery.VERSION, "status": "READY_FOR_PUBLICATION_RECOVERY", "run_id": self.request["run_id"],
               "namespace": self.request["namespace"], "original_runner_sha256": self.request["runner_sha256"], **self.pins,
               "month": "2016-04", "recovered": False, "acceptance_passed": False, "research_ready": False, "can_release_lease": False,
               "next_phases": ["verify", "publish"], "do_not_replay": list(e2e.PHASES[:4]), "remaining_case": "2023-12",
               "snapshots": {t: 123 for t in e2e.TABLES}, "checks": {"publication_absent": {"actual": 0, "expected": 0}}}
        for table in e2e.TABLES:
            for kind, count in (("current", 123), ("identity", 1)):
                out["checks"][kind + ":" + table] = {"actual": count, "expected": count, "sql": "synthetic"}
            out["checks"]["current:" + table]["sql"] = "SELECT snapshot_id FROM " + self.request["namespace"] + '."' + table + '$refs" WHERE name=\'main\''
        return out

    def failure_files(self, exit_code=1):
        e2e.save(self.root / "request.json", self.request)
        e2e.save(self.root / "execution.json", {"exit_code": exit_code, "lease_held_at_execution_end": True})
        return {"request_sha256": e2e.sha(self.root / "request.json"), "execution_sha256": e2e.sha(self.root / "execution.json"),
                "completed_progress_sha256": {m: "a" * 64 for m in recovery.COMPLETED}}

    def test_plan_is_not_recovery_acceptance_or_research_approval(self):
        recovery.validate_plan(self.plan(), self.pins, self.request)

    def test_no_completion_or_release_flag_can_be_promoted(self):
        for flag in ("recovered", "acceptance_passed", "research_ready", "can_release_lease"):
            value = self.plan(); value[flag] = True
            with self.assertRaisesRegex(RuntimeError, "PLAN_INVALID"):
                recovery.validate_plan(value, self.pins, self.request)

    def test_cannot_replay_committed_etl_or_skip_verify(self):
        for phases in (["bronze", "silver", "gold", "verify", "publish"], ["publish"]):
            value = self.plan(); value["next_phases"] = phases
            with self.assertRaises(RuntimeError):
                recovery.validate_plan(value, self.pins, self.request)

    def test_changed_runner_or_namespace_is_rejected(self):
        for key in ("preflight_runner_sha256", "original_runner_sha256", "namespace", "request_sha256"):
            value = self.plan(); value[key] = "changed"
            with self.assertRaises(RuntimeError):
                recovery.validate_plan(value, self.pins, self.request)

    def test_all_six_integer_snapshot_pins_and_thirteen_queries_required(self):
        for bad in (True, 0, 9223372036854775808, "123"):
            value = self.plan(); value["snapshots"]["event_current"] = bad
            with self.assertRaises(RuntimeError):
                recovery.validate_plan(value, self.pins, self.request)
        value = self.plan(); del value["checks"]["identity:event_current"]
        with self.assertRaises(RuntimeError):
            recovery.validate_plan(value, self.pins, self.request)

    def test_latest_query_cannot_replace_exact_main_ref_check(self):
        value = self.plan(); value["checks"]["current:event_current"]["sql"] = "SELECT 123"
        with self.assertRaisesRegex(RuntimeError, "SQL_CHANGED"):
            recovery.validate_plan(value, self.pins, self.request)

    def test_boolean_or_wrong_count_is_not_a_readback(self):
        for actual in (True, 2):
            value = self.plan(); value["checks"]["identity:event_current"]["actual"] = actual
            with self.assertRaises(RuntimeError):
                recovery.validate_plan(value, self.pins, self.request)

    def test_success_timeout_or_changed_execution_not_eligible(self):
        pins = self.failure_files(exit_code=0)
        with patch.object(e2e, "validate_inputs"), self.assertRaisesRegex(RuntimeError, "FAILED_EXECUTION_REQUIRED"):
            recovery.retained(self.root, pins)
        value = e2e.load(self.root / "execution.json"); value.pop("exit_code")
        (self.root / "execution.json").write_text(json.dumps(value)); pins["execution_sha256"] = e2e.sha(self.root / "execution.json")
        with patch.object(e2e, "validate_inputs"), self.assertRaisesRegex(RuntimeError, "FAILED_EXECUTION_REQUIRED"):
            recovery.retained(self.root, pins)

    def test_existing_acceptance_does_not_enter_recovery(self):
        pins = self.failure_files(); (self.root / "acceptance.json").write_text("{}")
        with patch.object(e2e, "validate_inputs"), self.assertRaisesRegex(RuntimeError, "UNEXPECTED_COMPLETION"):
            recovery.retained(self.root, pins)

    def test_changed_progress_or_input_pin_fails_before_docker(self):
        pins = self.failure_files(); (self.root / "progress-2000-01.json").write_text("{}")
        with patch.object(e2e, "validate_inputs") as check, self.assertRaisesRegex(RuntimeError, "PROGRESS_CHANGED"):
            recovery.retained(self.root, pins)
        check.assert_called_once()
        changed = copy.deepcopy(pins); changed["request_sha256"] = "e" * 64
        with self.assertRaisesRegex(RuntimeError, "ORIGINAL_CHANGED"):
            recovery.retained(self.root, changed)

    def test_idle_requires_both_no_dagrun_and_no_java(self):
        recovery.assert_idle(False, [])
        with self.assertRaisesRegex(RuntimeError, "ACTIVE_DAGRUN"):
            recovery.assert_idle(True, [])
        with self.assertRaisesRegex(RuntimeError, "ACTIVE_JAVA"):
            recovery.assert_idle(False, ["123"])

    def test_own_lease_is_readonly_foreign_or_missing_state_is_not_created(self):
        owner = {"dag_id": "dat03_bounded_e2e", "run_id": "dat03-" + "a" * 32}
        guard = self.root / "guard"; guard.mkdir()
        with patch.dict(os.environ, {"SOURCE_GUARD_ROOT": str(guard)}):
            with self.assertRaises(FileNotFoundError):
                recovery.assert_owner(owner)
            self.assertEqual([], list(guard.iterdir()))
            (guard / "mutex.lock").write_bytes(b""); e2e.save(guard / "owner.json", owner)
            before = (guard / "owner.json").read_bytes()
            recovery.assert_owner(owner)
            with self.assertRaisesRegex(RuntimeError, "LEASE_NOT_HELD"):
                recovery.assert_owner({**owner, "run_id": "foreign"})
            self.assertEqual(before, (guard / "owner.json").read_bytes())

    def test_attempt_and_symlink_are_not_overwritten(self):
        folder = self.root / "staging/dat03-recovery" / ("a" * 32); folder.mkdir(parents=True); (folder / "runtime").mkdir()
        with patch.object(recovery, "health", side_effect=AssertionError("Docker forbidden")), self.assertRaisesRegex(RuntimeError, "FRESH_ATTEMPT_REQUIRED"):
            recovery.check(self.root, folder)
        link = self.root / "link"; link.symlink_to(folder, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "SYMLINK_REJECTED"):
            recovery.check(self.root, link)


if __name__ == "__main__":
    unittest.main()
