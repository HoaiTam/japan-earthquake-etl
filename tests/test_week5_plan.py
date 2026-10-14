"""Offline regressions for week-5 assignment checks; never touches runtime data."""
from pathlib import Path
import os
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]


class Week5PlanTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.tasks = Path(self.temp.name) / "tasks"
        shutil.copytree(ROOT / "docs/task/tasks", self.tasks)

    def run_check(self):
        return subprocess.run(
            ["sh", str(ROOT / "scripts/check-week-5-plan.sh")],
            env={**os.environ, "WEEK5_TASKS_DIR": str(self.tasks)},
            capture_output=True, text=True, check=False,
        )

    def change(self, task, old, new):
        path = self.tasks / (task + ".md")
        content = path.read_text(encoding="utf-8")
        self.assertIn(old, content)
        path.write_text(content.replace(old, new, 1), encoding="utf-8")

    def rejects(self, reason):
        result = self.run_check()
        self.assertNotEqual(0, result.returncode)
        self.assertIn(reason, result.stderr)

    def test_current_plan_passes(self):
        result = self.run_check()
        self.assertEqual(0, result.returncode, result.stderr)

    def test_pilot_loader_dependency_required(self):
        self.change("EXP-01", ', "DAT-02"]', "]")
        self.rejects("EXP-01 lacks DAT-02 dependency")

    def test_official_export_gate_still_required(self):
        self.change("EXP-02", ', "MLD-05"]', "]")
        self.rejects("EXP-02 lacks MLD-05 dependency")

    def test_pilot_owner_is_hoaitam(self):
        self.change("DAT-02", 'assignee: "HoaiTam"', 'assignee: "rosy179"')
        self.rejects("DAT-02 owner")

    def test_recovery_is_scheduled_next_week(self):
        self.change("QA-04", "week: 6", "week: 5")
        self.rejects("expected 12 Core week-5 tasks")

    def test_feature_pilot_cycle_rejected(self):
        self.change("MLD-04", 'dependencies: ["MLD-03"]',
                    'dependencies: ["MLD-03", "DAT-02"]')
        self.rejects("MLD-04 creates pilot dependency cycle")

    def test_wrong_owner_fails(self):
        self.change("MLD-04", 'assignee: "rosy179"', 'assignee: "HoaiTam"')
        self.rejects("MLD-04 owner")

    def test_effort_drift_fails(self):
        self.change("ORC-06", "effort_hours: 8", "effort_hours: 9")
        self.rejects("ThanhTris expected 25h")

    def test_missing_adapter_gate_fails(self):
        self.change("QA-01", ', "ORC-07"]', "]")
        self.rejects("QA-01 lacks real adapter hard gate")

    def test_reviewer_exception_is_preserved(self):
        self.change("MLD-03", 'reviewer: "HoaiTam"', 'reviewer: "unassigned"')
        self.rejects("MLD-03 reviewer exception lost")

    def test_stretch_not_added_to_core_budget(self):
        self.change("BI-02", "week: 8", "week: 5")
        self.rejects("BI-02 must be deferred")


if __name__ == "__main__":
    unittest.main()

