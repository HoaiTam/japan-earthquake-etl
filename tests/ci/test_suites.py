"""Functional/task grouping must partition full coverage and preserve fail-closed gates."""
import importlib.util
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("ci_suites", ROOT / "scripts/ci-impact.py")
impact = importlib.util.module_from_spec(spec)
spec.loader.exec_module(impact)


class SuitePolicyTest(unittest.TestCase):
    def test_dat03_collection_and_bounded_runtime_have_consumer_suites(self):
        groups = impact.inventory("java")
        for name in ("ResearchCollectionPlanTest", "ResearchCollectionJobTest"):
            self.assertIn("ie212.earthquake.spark.ml." + name, groups["mld-05-export"])
        self.assertIn("ie212.earthquake.spark.usgs.ResearchBoundedE2EJobTest", groups["orc-07-real-adapters"])
        self.assertIn("ie212.earthquake.spark.usgs.ResearchCatalogRecoveryJobTest", groups["orc-07-real-adapters"])
        self.assertIn("test-research-e2e", (ROOT / "Makefile").read_text().split("ci-docs-base:", 1)[1])
        self.assertIn("test_research_catalog_recovery.py", (ROOT / "Makefile").read_text().split("ci-docs-base:", 1)[1])

    def test_strict_split_policy_regression_is_visible_in_window_suite_and_local_target(self):
        groups = impact.inventory("java")
        self.assertIn("ie212.earthquake.spark.ml.WindowCoveragePolicyTest", groups["mld-03-windows"])
        self.assertIn("WindowCoveragePolicyTest", (ROOT / "Makefile").read_text())

    def test_team_handoff_runs_python_pilot_without_unrelated_java_or_airflow(self):
        for path in impact.TEAM_PILOT_FILES:
            result = impact.classify([("M", path)])
            self.assertEqual("full", result["pilot"])
            self.assertEqual("none", result["java"])
            self.assertEqual("none", result["airflow"])
        self.assertIn("test_team_pilot.py", (ROOT / "Makefile").read_text())

    def test_full_java_inventory_covers_every_discovered_class_once(self):
        groups = impact.suite_plan(impact.full("fixture"), "java")
        found = [name for tests in groups.values() for name in tests]
        expected = {p.relative_to(ROOT / "spark/src/test/java").with_suffix("").as_posix().replace("/", ".")
                    for p in (ROOT / "spark/src/test/java").rglob("*.java")
                    if re.fullmatch(r"(?:Test.*|.*Tests?|.*TestCase)", p.stem)}
        self.assertEqual(len(found), len(set(found)))
        self.assertEqual(set(found), expected)
        self.assertEqual(set(groups), set(impact.JAVA_SUITES))

    def test_full_airflow_inventory_covers_every_module_once(self):
        groups = impact.suite_plan(impact.full("fixture"), "airflow")
        found = [name for tests in groups.values() for name in tests]
        self.assertEqual(len(found), len(set(found)))
        self.assertEqual(set(found), {p.name for p in (ROOT / "airflow/tests").glob("test_*.py")})
        self.assertEqual(set(groups), set(impact.AIRFLOW_SUITES))

    def test_feature_leaf_materializes_features_export_consumer_and_python_pilot(self):
        result = impact.classify([("M", "spark/src/main/java/ie212/earthquake/spark/ml/SpacetimeFeatureEngine.java")])
        groups = impact.suite_plan(result, "java")
        self.assertEqual(list(groups), ["mld-04-features", "mld-05-export"])
        self.assertEqual({name.rsplit(".", 1)[-1] for name in groups["mld-04-features"]}, impact.FEATURE_TESTS)
        self.assertEqual({name.rsplit(".", 1)[-1] for name in groups["mld-05-export"]}, impact.EXPORT_TESTS)
        self.assertEqual(impact.suite_plan(result, "airflow"), {})
        self.assertEqual(result["pilot"], "full")

    def test_window_contract_includes_all_three_ml_groups_and_control_consumers(self):
        result = impact.classify([("M", "spark/src/main/java/ie212/earthquake/spark/ml/MainshockWindowEngine.java")])
        self.assertEqual(set(impact.suite_plan(result, "java")),
                         {"mld-01-02-contract-audit", "mld-03-windows", "mld-04-features", "mld-05-export", "mli-02-build"})
        self.assertEqual(set(impact.suite_plan(result, "airflow")), set(impact.AIRFLOW_SUITES))

    def test_unknown_tests_are_never_lost_and_all_surefire_patterns_are_discovered(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            folder = root / "spark/src/test/java/newmodule"; folder.mkdir(parents=True)
            for name in ("TestNew", "NewTests", "NewTestCase", "NewTest", "NotATestHelper"):
                (folder / (name + ".java")).touch()
            python = root / "airflow/tests"; python.mkdir(parents=True)
            (python / "test_new_runtime.py").touch()
            groups = impact.suite_plan(impact.full("fixture"), "java", root)
            self.assertEqual(set(groups), {"foundation-integration"})
            self.assertEqual(len(groups["foundation-integration"]), 4)
            groups = impact.suite_plan(impact.full("fixture"), "airflow", root)
            self.assertEqual(groups, {"orchestration-recovery": ["test_new_runtime.py"]})

    def test_real_build_adapter_is_visible_in_task_suite(self):
        groups = impact.inventory("java")
        self.assertEqual(groups["mli-02-build"], [
            "ie212.earthquake.spark.ml.DatasetBuildJobTest",
            "ie212.earthquake.spark.usgs.ResearchDatasetBuildJobTest",
        ])
        self.assertTrue({"test_ml_dataset_real.py", "test_mli_02_build_dataset.py"} <= set(impact.inventory("airflow")["ml-qa"]))

    def test_empty_inventory_missing_selector_and_invalid_mode_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "EMPTY_TEST_INVENTORY"):
                impact.suite_plan(impact.full("fixture"), "java", Path(temporary))
        for tests in ([], ["unknown.MissingTest"]):
            with self.assertRaises(ValueError):
                impact.suite_plan(dict(impact.full("fixture"), java="selected", java_tests=tests), "java")
        with self.assertRaisesRegex(ValueError, "INVALID_PLAN"):
            impact.suite_plan(dict(impact.full("fixture"), airflow="unknown"), "airflow")

    def test_nested_python_test_fails_closed_instead_of_losing_coverage(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); nested = root / "airflow/tests/new_package"; nested.mkdir(parents=True)
            (nested / "test_nested.py").touch()
            with self.assertRaisesRegex(ValueError, "REQUIRES_MAPPING"):
                impact.suite_plan(impact.full("fixture"), "airflow", root)

    def test_docs_only_plan_has_no_worker_suites(self):
        result = impact.classify([("M", "docs/task/tasks/MLD-03.md")])
        for component in ("java", "airflow"):
            self.assertEqual(impact.suite_plan(result, component), {})
            run = Mock()
            with self.assertRaisesRegex(ValueError, "NOT_APPLICABLE"):
                impact.execute_suite(result, component, "not-applicable", runner=run)
            run.assert_not_called()

    def test_matrix_output_contains_only_fixed_group_ids_and_summary_counts(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"; summary = Path(temporary) / "summary"
            with patch("builtins.print") as printed:
                impact.emit(impact.full("fixture"), output, summary)
            self.assertEqual(json.loads(printed.call_args.args[0])["suites"]["java"],
                             {group: len(tests) for group, tests in impact.inventory("java").items()})
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
            for component, ids in (("java", impact.JAVA_SUITES), ("airflow", impact.AIRFLOW_SUITES)):
                self.assertEqual(json.loads(values[component + "_matrix"]), {"suite": list(ids)})
            self.assertIn("mld-03-windows", summary.read_text())
            self.assertIn("DAT-02/EXP-01/EXP-02/EXP-03 pilot, baselines & adaptive", summary.read_text())

    def test_java_suite_builds_and_checks_only_its_inventory_and_propagates_failures(self):
        run = Mock()
        result = impact.full("fixture")
        with patch.object(impact, "verify_selected_reports") as reports, patch("builtins.print"):
            impact.execute_suite(result, "java", "mld-03-windows", runner=run)
            expected = impact.inventory("java")["mld-03-windows"]
            reports.assert_called_once_with(ROOT, expected)
            command = run.call_args.args[0]
            self.assertIn("-Dtest=" + ",".join(expected), command)
            self.assertIn("clean", command); self.assertIn("verify", command)
            self.assertTrue(run.call_args.kwargs["check"])
            run.side_effect = RuntimeError("failure")
            with self.assertRaisesRegex(RuntimeError, "failure"):
                impact.execute_suite(result, "java", "gold", runner=run)
        with self.assertRaisesRegex(ValueError, "NOT_APPLICABLE"):
            impact.execute_suite(result, "java", "$(unsafe)", runner=run)

    def test_python_empty_module_and_import_failure_are_not_green(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); folder = root / "airflow/tests"; folder.mkdir(parents=True)
            (folder / "test_ci_unique_empty.py").write_text("# empty\n")
            with self.assertRaisesRegex(ValueError, "EMPTY_PYTHON_TEST_MODULE"), patch("builtins.print"):
                impact.execute_suite(impact.full("fixture"), "airflow", "orchestration-recovery", root)
        with patch.object(impact.unittest.TestLoader, "discover", side_effect=ImportError("broken import")), patch("builtins.print"):
            with self.assertRaises(ImportError):
                impact.execute_suite(impact.full("fixture"), "airflow", "ml-qa")
        report = Mock(); report.wasSuccessful.return_value = False
        with patch.object(impact.unittest.TextTestRunner, "run", return_value=report), patch("builtins.print"):
            with self.assertRaisesRegex(ValueError, "PYTHON_SUITE_FAILED"):
                impact.execute_suite(impact.full("fixture"), "airflow", "ml-qa")

    def test_gate_exhaustive_failure_skip_cancel_matrix(self):
        accepted = {("success", "none", "skipped"), ("success", "full", "success"), ("success", "selected", "success")}
        for planner in ("success", "failure", "cancelled", "skipped", ""):
            for mode in ("none", "full", "selected", "unknown", ""):
                for worker in ("success", "failure", "cancelled", "skipped", ""):
                    with self.subTest(planner=planner, mode=mode, worker=worker):
                        if (planner, mode, worker) in accepted:
                            impact.gate(planner, mode, worker)
                        else:
                            with self.assertRaisesRegex(ValueError, "DID_NOT_PASS"):
                                impact.gate(planner, mode, worker)

    def test_adaptive_hdbscan_changes_trigger_full_pilot_python(self):
        paths = [
            "notebooks/adaptive_hdbscan.py",
            "notebooks/exp_03_adaptive_hdbscan.ipynb",
            "scripts/inspect-exp03-notebook.py",
            "config/ml/models/hdbscan_adaptive_dev_v1.json",
            "tests/test_adaptive_hdbscan.py",
        ]
        for p in paths:
            with self.subTest(path=p):
                res = impact.classify([("M", p)])
                self.assertEqual(res["pilot"], "full")
                self.assertEqual(res["airflow"], "none")
                self.assertEqual(res["java"], "selected")


if __name__ == "__main__": unittest.main()
