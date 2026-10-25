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

    def test_new_plan_artifacts_have_one_final_newline(self):
        # Check file bytes, including new/untracked files that git diff may omit.
        paths = (
            "docs/specs/JUPYTER_PILOT_WORKFLOW.md",
            "docs/task/WEEK_5_PARALLEL_PLAN.md",
            "docs/task/tasks/DAT-02.md",
            "docs/task/tasks/ORC-06.md",
            "docs/task/tasks/ORC-07.md",
            "docs/task/tasks/DOC-01.md",
            "docs/task/tasks/QA-04.md",
            "docs/task/tasks/EXP-03.md",
            "docs/task/tasks/MLI-02.md",
            "scripts/check-week-5-plan.sh",
            "tests/test_week5_plan.py",
        )
        for relative in paths:
            with self.subTest(path=relative):
                content = (ROOT / relative).read_bytes()
                self.assertTrue(content.endswith(b"\n"), "missing final newline")
                self.assertFalse(content.endswith(b"\n\n"), "new blank line at EOF")

    def test_pilot_loader_dependency_required(self):
        self.change("EXP-01", ', "DAT-02"]', "]")
        self.rejects("EXP-01 lacks DAT-02 dependency")

    def test_official_export_gate_still_required(self):
        self.change("EXP-02", ', "MLD-05"]', "]")
        self.rejects("EXP-02 lacks MLD-05 dependency")

    def test_official_research_gate_does_not_disappear_after_component_acceptance(self):
        self.change("EXP-02", ', "DAT-03"', '')
        self.rejects("EXP-02 lacks DAT-03 dependency")

    def test_research_rollout_cannot_become_upstream_of_its_exporter(self):
        self.change("MLD-05", 'dependencies: ["MLD-04"]',
                    'dependencies: ["MLD-04", "DAT-03"]')
        self.rejects("MLD-05 creates research data dependency cycle")

    def test_pilot_owner_is_hoaitam(self):
        self.change("DAT-02", 'assignee: "HoaiTam"', 'assignee: "rosy179"')
        self.rejects("DAT-02 owner")

    def test_latest_assignment_weeks_cannot_revert_to_old_plan(self):
        for task, old_week in (("DOC-01", 6), ("QA-04", 6), ("EXP-03", 7)):
            with self.subTest(task=task):
                self.change(task, "week: 5", f"week: {old_week}")
                self.rejects(f"{task} week must be 5")
                self.change(task, f"week: {old_week}", "week: 5")

    def test_mli02_owner_tracks_latest_assignment(self):
        self.change("MLI-02", 'assignee: "rosy179"', 'assignee: "HoaiTam"')
        self.rejects("MLI-02 owner must be rosy179")

    def test_unassigned_extra_core_task_cannot_enter_week5(self):
        self.change("MLI-04", "week: 8", "week: 5")
        self.rejects("expected 15 Core week-5 tasks")

    def test_adaptive_dependency_is_not_removed_by_rescheduling(self):
        self.change("EXP-03", 'dependencies: ["EXP-02"]', 'dependencies: []')
        self.rejects("EXP-03 lacks EXP-02 dependency")

    def test_build_dag_keeps_real_export_dependency(self):
        self.change("MLI-02", ', "MLD-05"]', "]")
        self.rejects("MLI-02 lacks MLD-05 dependency")

    def test_feature_pilot_cycle_rejected(self):
        self.change("MLD-04", 'dependencies: ["MLD-03"]',
                    'dependencies: ["MLD-03", "DAT-02"]')
        self.rejects("MLD-04 creates pilot dependency cycle")

    def test_wrong_owner_fails(self):
        self.change("MLD-04", 'assignee: "rosy179"', 'assignee: "HoaiTam"')
        self.rejects("MLD-04 owner")

    def test_effort_drift_fails(self):
        self.change("ORC-06", "effort_hours: 8", "effort_hours: 9")
        self.rejects("HoaiTam expected 31h")

    def test_orc06_owner_tracks_user_takeover(self):
        self.change("ORC-06", 'assignee: "HoaiTam"', 'assignee: "ThanhTris"')
        self.rejects("ORC-06 owner must be HoaiTam")

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
