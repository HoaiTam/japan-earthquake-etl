"""Offline selection, failure propagation and real local-Git comparison regressions."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ci_impact", ROOT / "scripts/ci-impact.py")
impact = importlib.util.module_from_spec(spec); spec.loader.exec_module(impact)


class ImpactPolicyTest(unittest.TestCase):
    def choose(self, *paths): return impact.classify([("M", p) for p in paths])
    def test_task_markdown_skips_business_tests_but_keeps_guards(self):
        result = self.choose("docs/task/tasks/DAT-02.md", "docs/task/tasks/README.md", "README.md")
        self.assertEqual([result[k] for k in ["java", "airflow", "pilot"]], ["none"] * 3)
        self.assertEqual(result["docs"], "always")
    def test_notebook_and_python_loader_only_need_pilot_checks(self):
        result = self.choose("notebooks/pilot_bundle.py", "notebooks/dat_02_inspection.ipynb", "tests/test_pilot_bundle.py")
        self.assertEqual((result["java"], result["airflow"], result["pilot"]), ("none", "none", "full"))
    def test_feature_change_includes_exporter_and_independent_jvm_consumers(self):
        result = self.choose("spark/src/main/java/ie212/earthquake/spark/ml/SpacetimeFeatureEngine.java")
        self.assertEqual(result["java"], "selected"); self.assertEqual(result["airflow"], "none"); self.assertEqual(result["pilot"], "full")
        self.assertEqual({name.rsplit(".", 1)[-1] for name in result["java_tests"]}, impact.FEATURE_CONSUMER_TESTS)
    def test_window_or_dataset_contract_changes_expand_to_all_ml_and_control_plane(self):
        for name in ["MainshockWindowEngine", "DatasetManifestWriter", "NewMlProducer"]:
            with self.subTest(name=name):
                result = self.choose("spark/src/main/java/ie212/earthquake/spark/ml/" + name + ".java")
                self.assertEqual(result["airflow"], "full"); self.assertEqual(result["pilot"], "full")
                self.assertIn("ie212.earthquake.spark.ml.GoldInputAuditIntegrationTest", result["java_tests"])
                self.assertTrue(impact.FEATURE_TESTS.issubset({n.rsplit(".", 1)[-1] for n in result["java_tests"]}))
    def test_config_and_runtime_scripts_keep_java_python_boundary_checks(self):
        for name in sorted(impact.PILOT_CONFIGS | impact.PILOT_SCRIPTS):
            with self.subTest(path=name):
                result = self.choose(name)
                self.assertEqual((result["java"], result["pilot"]), ("selected", "full"))
    def test_leaf_dag_and_python_test_run_all_control_plane_tests(self):
        result = self.choose("airflow/dags/orc_02_daily_sources.py", "airflow/tests/test_source_schedule_runtime.py")
        self.assertEqual((result["java"], result["airflow"], result["pilot"]), ("none", "full", "none"))
    def test_shared_and_unmapped_paths_fail_safe_to_full(self):
        for name in ["pom.xml", "spark/pom.xml", "Makefile", ".github/workflows/ci.yml", "scripts/ci-impact.py",
                     "tests/ci/test_impact.py", "docs/specs/ML_DATA_MODEL.md", "compose.yaml", ".env.example",
                     "notebooks/requirements-pilot.txt",
                     "tests/fixtures/README.md", "spark/src/test/resources/ml/new.json", "spark/src/main/resources/new.json",
                     "spark/src/main/java/ie212/earthquake/spark/silver/SilverParquetSerializer.java",
                     "spark/src/main/java/ie212/earthquake/spark/gold/GoldPublisher.java",
                     "airflow/dags/source_run_guard.py", "airflow/dags/etl_pipeline_runtime.py", "airflow/dags/new_dag.py",
                     "notebooks/new_model.py", "docs/evidence/data.json", "new-root-file", "--flag", "docs/a\nline.md"]:
            with self.subTest(path=name): self.assertEqual([self.choose(name)[k] for k in ["java", "airflow", "pilot"]], ["full"] * 3)
    def test_union_of_scopes_never_overrides_full_or_drops_python(self):
        selected = "spark/src/main/java/ie212/earthquake/spark/ml/FeatureScalingConfig.java"
        result = self.choose(selected, "airflow/dags/afl_01_smoke.py")
        self.assertEqual((result["java"], result["airflow"], result["pilot"]), ("selected", "full", "full"))
        self.assertEqual(self.choose(selected, "Makefile")["java"], "full")
    def test_empty_deleted_renamed_type_changed_or_oversized_comparison_runs_full(self):
        for changes in [[], [("D", "docs/a.md")], [("R100", "docs/a.md")], [("T", "docs/a.md")], [("M", "README.md")] * 10001]:
            self.assertEqual(impact.classify(changes)["java"], "full")
    def test_missing_expected_tests_does_not_silently_skip(self):
        with tempfile.TemporaryDirectory() as root:
            result = impact.classify([("M", "config/ml/dat02_reproduction_v1.json")], Path(root))
        self.assertEqual(result["java"], "full")
    def test_new_ml_tests_follow_all_default_surefire_naming_patterns(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); folder = root / impact.JAVA_ROOT / "ml"; folder.mkdir(parents=True)
            for name in impact.FEATURE_CONSUMER_TESTS | {"TestNew", "NewTests", "NewTestCase"}: (folder / (name + ".java")).touch()
            result = impact.classify([("A", "spark/src/test/java/ie212/earthquake/spark/ml/NewTests.java")], root)
            self.assertEqual(len(result["java_tests"]), len(impact.FEATURE_CONSUMER_TESTS) + 3)
    def test_only_fixed_modes_are_emitted_to_actions_output(self):
        with tempfile.TemporaryDirectory() as root:
            output, summary = Path(root) / "output", Path(root) / "summary"
            with patch("builtins.print"): impact.emit(self.choose("README.md"), output, summary)
            lines = output.read_text().splitlines()
            self.assertEqual(lines[:3], ["java=none", "airflow=none", "pilot=none"])
            self.assertEqual(json.loads(lines[3].split("=", 1)[1]), {"suite": ["not-applicable"]})
            self.assertEqual(json.loads(lines[4].split("=", 1)[1]), {"suite": ["not-applicable"]})
            self.assertIn("not applicable", summary.read_text())
    def test_no_commands_for_not_applicable_and_failures_propagate(self):
        run = Mock()
        with patch("builtins.print"): impact.execute(self.choose("README.md"), "java", runner=run)
        run.assert_not_called()
        run.side_effect = subprocess.CalledProcessError(1, ["make"])
        with self.assertRaises(subprocess.CalledProcessError): impact.execute(impact.full("test"), "airflow", runner=run)
        self.assertTrue(run.call_args.kwargs["check"])
    def test_selected_command_compiles_packages_and_validates_every_test_report(self):
        result = self.choose("scripts/local-ml.py")
        command = impact.commands(result, "java")[0]
        self.assertEqual(command[0], "./mvnw"); self.assertIn("clean", command); self.assertIn("verify", command)
        self.assertTrue(any("PilotExportJobTest" in s for s in command))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); reports = root / "spark/target/surefire-reports"; reports.mkdir(parents=True)
            with self.assertRaisesRegex(ValueError, "MISSING"): impact.verify_selected_reports(root, ["a.Test"])
            report = reports / "TEST-a.Test.xml"
            for xml in ['<testsuite tests="0"/>', '<testsuite tests="1" skipped="1"/>', '<testsuite tests="1" failures="1"/>']:
                report.write_text(xml)
                with self.assertRaisesRegex(ValueError, "DID_NOT_PASS"): impact.verify_selected_reports(root, ["a.Test"])
            report.write_text('<testsuite tests="1" failures="0" errors="0"/>')
            impact.verify_selected_reports(root, ["a.Test"])


class GitComparisonTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.git("init", "-q"); self.git("config", "user.name", "CI fixture"); self.git("config", "user.email", "ci-fixture@example.invalid")
        self.commit("README.md", "base\n"); self.base = self.git("rev-parse", "HEAD").strip()
    def tearDown(self): self.temp.cleanup()
    def git(self, *args): return subprocess.check_output(["git", "-C", str(self.root), *args], text=True, stderr=subprocess.DEVNULL)
    def commit(self, name, text):
        path = self.root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_text(text)
        self.git("add", "--", name); self.git("commit", "-qm", "fixture")
    def test_pr_uses_local_diff_of_checked_out_merge_commit_against_actual_base(self):
        self.commit("docs/task/tasks/DAT-02.md", "documentation\n")
        result = impact.plan("pull_request", self.base, "HEAD", self.root)
        self.assertEqual(result["java"], "none"); self.assertEqual(result["change_count"], 1)
    def test_stack_base_excludes_already_present_java_changes(self):
        self.commit("shared.java", "upstream change\n"); stacked_base = self.git("rev-parse", "HEAD").strip()
        self.commit("README.md", "downstream docs\n")
        self.assertEqual(impact.plan("pull_request", stacked_base, "HEAD", self.root)["java"], "none")
        self.assertEqual(impact.plan("pull_request", self.base, "HEAD", self.root)["java"], "full")
    def test_merge_ref_excludes_changes_already_in_advanced_base_branch(self):
        self.git("switch", "-qc", "feature")
        self.commit("docs/task/tasks/DAT-02.md", "PR docs\n")
        self.git("switch", "-qc", "target", self.base)
        self.commit("shared.java", "base advanced independently\n")
        new_base = self.git("rev-parse", "HEAD").strip()
        self.git("merge", "--no-ff", "-qm", "simulated merge ref", "feature")
        result = impact.plan("pull_request", new_base, "HEAD", self.root)
        self.assertEqual(result["java"], "none"); self.assertEqual(result["change_count"], 1)
    def test_main_manual_unrecognized_and_missing_context_always_run_full(self):
        for event in ["push", "workflow_dispatch", "local", "unrecognized"]:
            self.assertEqual(impact.plan(event, self.base, "HEAD", self.root)["java"], "full")
        for base in ["", "0" * 40, "missing-commit", "--help"]:
            self.assertEqual(impact.plan("pull_request", base, "HEAD", self.root)["java"], "full")
        self.assertEqual(impact.plan("pull_request", self.base, "HEAD", self.root, force_full=True)["java"], "full")
    def test_wrong_head_empty_diff_and_incomplete_history_are_full(self):
        self.assertEqual(impact.plan("pull_request", self.base, "HEAD", self.root)["java"], "full")
        self.commit("README.md", "changed\n")
        self.assertEqual(impact.plan("pull_request", self.base, self.base, self.root)["java"], "full")
        self.assertEqual(impact.plan("pull_request", "missing-history", "HEAD", self.root)["java"], "full")
    def test_rename_and_deleted_scope_are_not_lost(self):
        self.git("mv", "README.md", "docs.md"); self.git("commit", "-qm", "rename")
        self.assertEqual(impact.plan("pull_request", self.base, "HEAD", self.root)["java"], "full")
    def test_more_than_api_three_hundred_file_limit_uses_full_local_diff(self):
        for i in range(305):
            path = self.root / ("docs/file" + str(i) + ".md"); path.parent.mkdir(exist_ok=True); path.write_text("doc\n")
        (self.root / "zz-new-code.py").write_text("runtime\n")
        self.git("add", "--", "docs", "zz-new-code.py"); self.git("commit", "-qm", "many files")
        result = impact.plan("pull_request", self.base, "HEAD", self.root)
        self.assertEqual(result["java"], "full"); self.assertEqual(result["change_count"], 306)


if __name__ == "__main__": unittest.main()
