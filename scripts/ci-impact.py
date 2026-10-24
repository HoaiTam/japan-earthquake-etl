#!/usr/bin/env python3
"""Conservative CI impact selection. No network, task-label trust or shell interpolation."""
import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
JAVA_ROOT = "spark/src/test/java/ie212/earthquake/spark/"
FEATURE_SOURCES = {"FeatureScalingConfig", "SpacetimeFeatureEngine", "LocalFeatureJob", "PilotExportJob", "WindowArtifactInput"}
FEATURE_TESTS = {"FeatureScalingConfigTest", "SpacetimeFeatureEngineTest", "PilotExportJobTest", "FeatureParquetCrossProcessTest"}
EXPORT_TESTS = {"FeatureExportCoverageTest", "FeatureExportBundleTest"}
FEATURE_CONSUMER_TESTS = FEATURE_TESTS | EXPORT_TESTS
PILOT_SCRIPTS = {"scripts/local-ml.py", "scripts/stage-ml-windows.py", "scripts/inspect-pilot-notebook.py", "scripts/pack-pilot.py"}
PILOT_CONFIGS = {"config/ml/feature_scaling_global_dev_v1.json", "config/ml/feature_scaling_adaptive_dev_v1.json", "config/ml/dat02_reproduction_v1.json"}
PILOT_FILES = {"notebooks/pilot_bundle.py", "notebooks/dat_02_inspection.ipynb"}
TEAM_PILOT_FILES = {"scripts/pack-team-pilot.py", "scripts/verify-team-pilot.py", "tests/test_team_pilot.py"}
EXP_FILES = {
    "notebooks/experiment_harness.py",
    "notebooks/baseline_models.py",
    "notebooks/adaptive_hdbscan.py",
    "notebooks/exp_01_harness_smoke.ipynb",
    "notebooks/exp_02_baseline_algorithms.ipynb",
    "notebooks/exp_03_adaptive_hdbscan.ipynb",
}
EXP_SCRIPTS = {
    "scripts/inspect-exp01-notebook.py",
    "scripts/inspect-exp02-notebook.py",
    "scripts/inspect-exp03-notebook.py",
}
EXP_CONFIGS = {
    "config/ml/models/window_baseline_v1.json",
    "config/ml/models/dbscan_global_dev_v1.json",
    "config/ml/models/hdbscan_global_dev_v1.json",
    "config/ml/models/hdbscan_adaptive_dev_v1.json",
}
LEAF_DAGS = {"airflow/dags/" + name + ".py" for name in
             ("afl_01_smoke", "usg_04_usgs_ingest", "jma_04_year_backfill", "orc_01_etl_pipeline", "orc_02_daily_sources", "orc_03_backfill")}
WINDOW_TESTS = {"MainshockRuntimeTest", "MainshockWindowEngineTest", "MainshockWindowIntegrationTest",
                "MainshockWindowRegressionTest", "WindowModelResolverTest", "WindowCoveragePolicyTest"}
JAVA_SUITES = ("foundation-integration", "usgs-bronze", "jma-bronze", "silver", "gold", "orc-06-full-month", "orc-07-real-adapters",
               "mld-01-02-contract-audit", "mld-03-windows", "mld-04-features", "mld-05-export", "mli-02-build")
AIRFLOW_SUITES = ("source-ingestion", "orchestration-recovery", "silver-gold-qa", "ml-qa")
SOURCE_TESTS = {"test_smoke_dag_contract", "test_usg_04_dag_contract", "test_usgs_ingest_runtime",
                "test_jma_backfill_runtime", "test_jma_dag_contract", "test_jma_live_qa"}
LAKEHOUSE_TESTS = {"test_silver_integration_qa", "test_gold_storage_qa", "test_gold_publication_qa"}
ML_QA_TESTS = {"test_dataset_manifest_qa", "test_gold_input_audit_qa", "test_mainshock_window_qa", "test_feature_export_runtime",
               "test_mli_02_build_dataset", "test_ml_dataset_real"}


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, check=True, timeout=30).stdout


def full(reason, count=0):
    return {"policy_version": "impact-v2", "java": "full", "airflow": "full", "pilot": "full", "java_tests": [],
            "change_count": count, "reasons": [reason], "docs": "always"}


def classify(changes, root=ROOT):
    """Only reviewed leaf paths are selective. Every unknown path expands to full regression."""
    if not changes: return full("empty-comparison")
    if len(changes) > 10000: return full("comparison-limit", len(changes))
    scopes, reasons = set(), set()
    airflow, pilot = "none", "none"
    for status, path in changes:
        parts = PurePosixPath(path).parts
        if status not in {"A", "M"}: return full("deleted-renamed-or-type-changed", len(changes))
        if not re.fullmatch(r"[A-Za-z0-9_./-]+", path) or path.startswith("/") or ".." in parts:
            return full("unusual-path", len(changes))
        # Contract specifications may influence readers and SQL even without code changes.
        if path.startswith(("docs/specs/", "docs/contracts/")): return full("shared-contract", len(changes))
        if path in {"README.md", "LICENSE", "LICENSE.md"} or (path.startswith("docs/") and path.endswith(".md")):
            reasons.add("documentation"); continue
        if path in TEAM_PILOT_FILES:
            pilot = "full"; reasons.add("team-pilot-handoff"); continue
        if (
            path in PILOT_FILES
            or path in PILOT_SCRIPTS
            or path in EXP_FILES
            or path in EXP_SCRIPTS
            or re.fullmatch(r"tests/test_(pilot_[a-z0-9_]+|experiment_harness|baseline_algorithms|adaptive_hdbscan)\.py", path)
        ):
            pilot = "full"
            reasons.add("pilot-python")
            if path in PILOT_SCRIPTS:
                scopes.add("feature")
            elif path in EXP_FILES or path in EXP_SCRIPTS or any(k in path for k in ("experiment_harness", "baseline_algorithms", "adaptive_hdbscan")):
                scopes.add("ml")
            continue
        if path in PILOT_CONFIGS or path in EXP_CONFIGS:
            scopes.add("feature" if path in PILOT_CONFIGS else "ml")
            pilot = "full"
            reasons.add("feature-config" if path in PILOT_CONFIGS else "experiment-config")
            continue
        match = re.fullmatch(r"spark/src/(main|test)/java/ie212/earthquake/spark/ml/([A-Za-z0-9_]+)\.java", path)
        if match:
            kind, name = match.groups()
            if (kind == "main" and name in FEATURE_SOURCES) or (kind == "test" and name in FEATURE_TESTS):
                scopes.add("feature"); reasons.add("feature-leaf")
            else:
                scopes.add("ml"); airflow = "full"; reasons.add("ml-contract-consumers")
            pilot = "full"; continue
        if path in LEAF_DAGS or re.fullmatch(r"airflow/tests/test_[a-z0-9_]+\.py", path):
            airflow = "full"; reasons.add("airflow")
            # All Python control-plane consumers are tested, but unchanged Java is not rebuilt.
            continue
        # Silver/Gold/Bronze/USGS/JMA, root entrypoints, fixtures, schemas, build,
        # CI tooling, new experiment modules and unreviewed files are shared/fallback.
        return full("shared-or-unmapped-path", len(changes))
    tests = []
    if scopes:
        paths = [p for p in sorted((root / JAVA_ROOT / "ml").glob("*.java"))
                 if re.fullmatch(r"(?:Test.*|.*Tests?|.*TestCase)", p.stem)]
        if "ml" not in scopes: paths = [p for p in paths if p.stem in FEATURE_CONSUMER_TESTS]
        if not paths or not FEATURE_CONSUMER_TESTS.issubset({p.stem for p in paths}): return full("missing-selected-test-coverage", len(changes))
        for path in paths:
            name = path.relative_to(root / "spark/src/test/java").with_suffix("").as_posix().replace("/", ".")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+", name):
                return full("invalid-test-selector", len(changes))
            tests.append(name)
    return {"policy_version": "impact-v2", "java": "selected" if tests else "none", "airflow": airflow, "pilot": pilot,
            "java_tests": tests, "change_count": len(changes), "reasons": sorted(reasons), "docs": "always"}


def resolve(root, ref):
    if not ref or "\n" in ref or "\0" in ref: raise ValueError("missing comparison revision")
    return git(root, "rev-parse", "--verify", "--end-of-options", ref + "^{commit}").decode().strip()


def plan(event, base, head, root=ROOT, force_full=False):
    if force_full or event != "pull_request": return full("forced-or-non-pr-event")
    try:
        before, after, checked_out = resolve(root, base), resolve(root, head), resolve(root, "HEAD")
        if after != checked_out: return full("head-not-checked-out")
        git(root, "merge-base", "--is-ancestor", before, after)
        raw = git(root, "diff", "--name-status", "--no-renames", "-z", before, after, "--")
        if len(raw) > 2_097_152: return full("comparison-limit")
        entries = raw.decode("utf-8").split("\0")
        if entries[-1] != "" or (len(entries) - 1) % 2: return full("invalid-comparison")
        changes = list(zip(entries[:-1:2], entries[1:-1:2]))
        result = classify(changes, root); result.update(base_sha=before, head_sha=after)
        return result
    except (subprocess.SubprocessError, OSError, UnicodeError, ValueError):
        return full("comparison-unavailable")


def inventory(component, root=ROOT):
    """Partition every discovered test once; new/unmapped tests join a shared suite."""
    if component == "java":
        groups = {name: [] for name in JAVA_SUITES}
        test_root = root / "spark/src/test/java"
        for path in sorted(test_root.rglob("*.java")):
            if not re.fullmatch(r"(?:Test.*|.*Tests?|.*TestCase)", path.stem): continue
            relative = path.relative_to(test_root).with_suffix("")
            name = relative.as_posix().replace("/", ".")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+", name):
                raise ValueError("CI_INVALID_TEST_NAME")
            package = relative.parent.as_posix()
            group = "foundation-integration"
            if package == "ie212/earthquake/spark/orchestration":
                group = "orc-07-real-adapters" if path.stem == "RealEtlAdapterTest" else "orc-06-full-month"
            elif package == "ie212/earthquake/spark/ml":
                group = ("mli-02-build" if path.stem == "DatasetBuildJobTest" else
                         "mld-05-export" if path.stem in EXPORT_TESTS else "mld-04-features" if path.stem in FEATURE_TESTS else
                         "mld-03-windows" if path.stem in WINDOW_TESTS else "mld-01-02-contract-audit")
            elif package in {"ie212/earthquake/spark/" + p for p in ("usgs", "jma", "silver", "gold")}:
                group = {"usgs": "usgs-bronze", "jma": "jma-bronze", "silver": "silver", "gold": "gold"}[relative.parent.name]
            groups[group].append(name)
    elif component == "airflow":
        groups = {name: [] for name in AIRFLOW_SUITES}
        test_root = root / "airflow/tests"
        for path in sorted(test_root.rglob("test_*.py")):
            # Nested unittest packages need an explicit import/discovery policy first.
            # Fail rather than silently omit their tests from a full matrix.
            if path.parent != test_root: raise ValueError("CI_NESTED_PYTHON_TEST_REQUIRES_MAPPING")
            if not re.fullmatch(r"test_[a-z0-9_]+", path.stem): raise ValueError("CI_INVALID_TEST_NAME")
            group = ("source-ingestion" if path.stem in SOURCE_TESTS else
                     "silver-gold-qa" if path.stem in LAKEHOUSE_TESTS else
                     "ml-qa" if path.stem in ML_QA_TESTS else "orchestration-recovery")
            groups[group].append(path.name)
    else:
        raise ValueError("CI_INVALID_COMPONENT")
    return groups


def suite_plan(result, component, root=ROOT):
    mode = result[component]
    if mode == "none": return {}
    groups = inventory(component, root)
    if mode == "selected" and component == "java":
        requested = set(result["java_tests"])
        if not requested: raise ValueError("CI_EMPTY_SELECTED_TESTS")
        discovered = {test for tests in groups.values() for test in tests}
        if not requested.issubset(discovered): raise ValueError("CI_SELECTED_TEST_NOT_DISCOVERED")
        groups = {group: [test for test in tests if test in requested] for group, tests in groups.items()}
    elif mode != "full":
        raise ValueError("CI_INVALID_PLAN")
    groups = {group: tests for group, tests in groups.items() if tests}
    if not groups: raise ValueError("CI_EMPTY_TEST_INVENTORY")
    return groups


def gate(planner_result, mode, suite_result):
    """A skipped/failed/cancelled applicable suite must never become a green gate."""
    if planner_result != "success" or (mode, suite_result) not in {
            ("none", "skipped"), ("full", "success"), ("selected", "success")}:
        raise ValueError("CI_REQUIRED_SUITE_DID_NOT_PASS")


def emit(result, output=None, summary=None, root=ROOT):
    matrices = {component: suite_plan(result, component, root) for component in ("java", "airflow")}
    diagnostic = dict(result, suites={component: {group: len(tests) for group, tests in groups.items()}
                                      for component, groups in matrices.items()})
    print(json.dumps(diagnostic, sort_keys=True), flush=True)
    if output:
        # Only fixed modes and allowlisted group IDs; never raw changed paths/titles.
        with Path(output).open("a") as file:
            for component in ("java", "airflow", "pilot"): file.write(component + "=" + result[component] + "\n")
            for component, groups in matrices.items():
                # A non-empty placeholder keeps matrix parsing valid; job-level mode skips it.
                file.write(component + "_matrix=" + json.dumps({"suite": list(groups) or ["not-applicable"]}) + "\n")
    if summary:
        with Path(summary).open("a") as file:
            file.write("### Impact-based CI\n\nDocs/contracts/security: always. "
                       f"Java: **{result['java']}** ({len(result['java_tests'])} selected classes); "
                       f"Airflow: **{result['airflow']}**; pilot: **{result['pilot']}**.\n\n"
                       f"Changed paths: {result['change_count']}; reasons: {', '.join(result['reasons'])}.\n\n"
                       "`none` means not applicable to this comparison, not a suppressed failure.\n")
            file.write("\n| Component | Functional/task suite | Test classes/modules |\n|---|---|---|\n")
            for component, groups in matrices.items():
                for group, tests in groups.items(): file.write(f"| {component} | {group} | {len(tests)} |\n")
            if result["pilot"] != "none": file.write("| Python | DAT-02/EXP-01/EXP-02/EXP-03 pilot, baselines & adaptive + DAT-03 handoff | test_pilot_*.py, test_team_pilot.py, test_experiment_harness.py, test_baseline_algorithms.py, test_adaptive_hdbscan.py |\n")


def commands(result, component):
    mode = result[component]
    if mode == "none": return []
    if component == "pilot": return [["make", "test-pilot", "PILOT_PYTHON=" + sys.executable]]
    if component == "airflow": return [["make", "ci-airflow"]]
    if mode == "full": return [["make", "ci-java"]]
    if mode != "selected" or not result["java_tests"]: raise ValueError("CI_INVALID_PLAN")
    return [["./mvnw", "--batch-mode", "--no-transfer-progress", "-pl", "spark", "-am", "clean", "verify",
             "-Dtest=" + ",".join(result["java_tests"]), "-Dsurefire.failIfNoSpecifiedTests=false", "-DfailIfNoTests=true"]]


def verify_selected_reports(root, tests):
    for name in tests:
        path = root / "spark/target/surefire-reports" / ("TEST-" + name + ".xml")
        if not path.is_file(): raise ValueError("CI_SELECTED_TEST_REPORT_MISSING:" + name)
        report = ET.parse(path).getroot()
        total, skipped = int(report.attrib["tests"]), int(report.attrib.get("skipped", 0))
        if total < 1 or skipped >= total or int(report.attrib.get("failures", 0)) or int(report.attrib.get("errors", 0)):
            raise ValueError("CI_SELECTED_TEST_DID_NOT_PASS:" + name)


def execute(result, component, root=ROOT, runner=subprocess.run):
    selected = commands(result, component)
    if not selected:
        print("event=ci_not_applicable component=" + component, flush=True); return
    env = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1", SPARK_LOCAL_HOSTNAME="localhost")
    for command in selected: runner(command, cwd=root, env=env, check=True)
    if component == "java" and result[component] == "selected": verify_selected_reports(root, result["java_tests"])


def execute_suite(result, component, suite, root=ROOT, runner=subprocess.run):
    groups = suite_plan(result, component, root)
    if suite not in groups: raise ValueError("CI_SUITE_NOT_APPLICABLE")
    tests = groups[suite]
    print(f"event=ci_suite_started component={component} suite={suite} test_units={len(tests)}", flush=True)
    if component == "java":
        selected = dict(result, java="selected", java_tests=tests)
        execute(selected, "java", root, runner)
    else:
        # Match full unittest discovery semantics while rejecting empty modules and import failures.
        loader = unittest.TestLoader()
        collection = unittest.TestSuite()
        for test in tests:
            found = loader.discover(str(root / "airflow/tests"), pattern=test)
            if found.countTestCases() < 1: raise ValueError("CI_EMPTY_PYTHON_TEST_MODULE:" + test)
            collection.addTests(found)
        report = unittest.TextTestRunner(verbosity=2).run(collection)
        if not report.wasSuccessful(): raise ValueError("CI_PYTHON_SUITE_FAILED")
    print(f"event=ci_suite_passed component={component} suite={suite}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["plan", "run", "run-suite", "gate"])
    parser.add_argument("--event", default=os.environ.get("CI_EVENT_NAME", "local"))
    parser.add_argument("--base", default=os.environ.get("CI_BASE_SHA", ""))
    parser.add_argument("--head", default=os.environ.get("CI_HEAD_SHA", "HEAD"))
    parser.add_argument("--force-full", action="store_true")
    parser.add_argument("--component", choices=["java", "airflow", "pilot"], default=os.environ.get("CI_COMPONENT"))
    parser.add_argument("--suite", default=os.environ.get("CI_SUITE"))
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    if args.action == "gate":
        gate(os.environ.get("CI_PLANNER_RESULT"), os.environ.get("CI_MODE"), os.environ.get("CI_SUITE_RESULT"))
        print("event=ci_gate_passed", flush=True)
        return
    if args.action in {"run", "run-suite"} and not args.component: parser.error("run requires --component")
    if args.action == "run-suite" and (args.component not in {"java", "airflow"} or not args.suite):
        parser.error("run-suite requires Java/Airflow and --suite")
    result = plan(args.event, args.base, args.head, force_full=args.force_full)
    emit(result, args.github_output, args.summary)
    if args.action == "run": execute(result, args.component)
    elif args.action == "run-suite": execute_suite(result, args.component, args.suite)


if __name__ == "__main__": main()
