"""Week-7 docs/ownership/setup regressions, stdlib only; no cloud/runtime writes."""
import ast
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[1]
TASKS = ROOT / "docs/task/tasks"
SETUP = ROOT / "docs/runbooks/WEEK_7_COLAB_SETUP.md"
PLAN = ROOT / "docs/task/WEEK_7_COLAB_PLAN.md"


def field(task_id, key):
    source = (TASKS / f"{task_id}.md").read_text()
    match = re.search(rf'^{re.escape(key)}: "?([^"\n]+)"?$', source, re.M)
    if not match:
        raise AssertionError(f"Missing {key} in {task_id}")
    return match[1]


class Week7PlanTest(unittest.TestCase):
    def test_all_twelve_tasks_have_requested_owners_and_plan_links(self):
        owners = {
            "HoaiTam": ("DAT-03", "EXP-01", "COL-01", "DOC-01", "SEC-01"),
            "rosy179": ("EXP-02", "COL-02", "COL-03"),
            "ThanhTris": ("EXP-03", "COL-04", "EXP-04", "MLI-04"),
        }
        plan = PLAN.read_text()
        for owner, tasks in owners.items():
            for task in tasks:
                with self.subTest(task=task):
                    self.assertEqual(owner, field(task, "assignee"))
                    self.assertIn(f"[{task}](./tasks/{task}.md)", plan)

    def test_new_tasks_have_valid_tracking_without_freezing_future_progress(self):
        for task in ("COL-01", "COL-02", "COL-03", "COL-04"):
            with self.subTest(task=task):
                status = field(task, "status")
                self.assertIn(status, {"Backlog", "Ready", "In Progress", "Review", "Blocked", "Needs Update", "Done"})
                self.assertEqual("7", field(task, "week"))
                self.assertEqual("Core", field(task, "scope"))
                text = (TASKS / f"{task}.md").read_text()
                self.assertIn(f"**Trạng thái:** {status}", text)
                if status == "Backlog":
                    self.assertIn("Chưa triển khai", text)
                self.assertIn("WEEK_7_COLAB_SETUP.md", text)

    def test_backlog_totals_and_addendum_preserve_original_estimates(self):
        files = list(TASKS.glob("*-[0-9][0-9].md"))
        self.assertEqual(81, len(files))
        hours = sum(int(field(path.stem, "effort_hours")) for path in files)
        self.assertEqual(416, hours)
        self.assertEqual(14, sum(int(field(f"COL-0{i}", "effort_hours")) for i in range(1, 5)))
        self.assertEqual(364, hours - 16 - 6 - 16 - 14)

    def test_hard_scientific_gates_not_lowered_by_pilot_plan(self):
        for task, dependencies in {
            "EXP-02": ("DAT-03", "MLD-05"),
            "EXP-03": ("EXP-02",),
            "EXP-04": ("EXP-02", "EXP-03"),
            "MLI-04": ("MLI-03", "EXP-04"),
        }.items():
            frontmatter = (TASKS / f"{task}.md").read_text().split("---", 2)[1]
            for dependency in dependencies:
                self.assertIn(f'"{dependency}"', frontmatter)
        plan = PLAN.read_text()
        self.assertIn("chưa đủ chuyển toàn task Done", plan)
        self.assertIn("không thay receipt từ máy riêng", plan)

    def test_setup_python_cells_compile(self):
        blocks = re.findall(r"```python\n(.*?)```", SETUP.read_text(), re.S)
        self.assertGreaterEqual(len(blocks), 6)
        for number, block in enumerate(blocks, 1):
            with self.subTest(cell=number):
                compile(block, f"<week7-setup-cell-{number}>", "exec")

    def test_setup_calls_existing_apis_with_valid_keyword_names(self):
        signatures = {}
        for module in ("baseline_models", "adaptive_hdbscan"):
            tree = ast.parse((ROOT / "notebooks" / f"{module}.py").read_text())
            for node in tree.body:
                if isinstance(node, ast.FunctionDef):
                    signatures[node.name] = {arg.arg for arg in node.args.args + node.args.kwonlyargs}
        for block in re.findall(r"```python\n(.*?)```", SETUP.read_text(), re.S):
            for node in ast.walk(ast.parse(block)):
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {
                    "run_baseline_experiment", "run_adaptive_experiment",
                }:
                    self.assertTrue({kw.arg for kw in node.keywords}.issubset(signatures[node.func.id]))

    def test_setup_has_trusted_pins_and_no_auto_pass_or_destructive_commands(self):
        setup = SETUP.read_text()
        handoff = (ROOT / "docs/runbooks/DAT-03_DRIVE_DATA_HANDOFF.md").read_text()
        for digest in (
            "f58291b72873e1b26791fef97818fdcf5e2b028a5dfa528885a666d8cdc1b870",
            "9121d6839f9ef8e5646a06cd0bf870fcde50b65da00c03e7b268760237cddc9a",
        ):
            self.assertIn(digest, setup)
            self.assertIn(digest, handoff)
        self.assertIn("runtime_kind", setup)
        self.assertIn("GOOGLE_COLAB", setup)
        self.assertIn("không thay receipt local", setup)
        for block in re.findall(r"```[^\n]*\n(.*?)```", setup, re.S):
            self.assertNotRegex(block, r"docker compose down|git reset --hard|\brm\s+-[a-z]*f")
            self.assertNotIn("EXP02_FRESH_KERNEL_ACCEPTANCE", block)
            self.assertNotIn("EXP03_FRESH_KERNEL_ACCEPTANCE", block)


if __name__ == "__main__":
    unittest.main()
