#!/usr/bin/env python3
"""Conservative CI impact selection. No network, task-label trust or shell interpolation."""
import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
JAVA_ROOT = "spark/src/test/java/ie212/earthquake/spark/"
FEATURE_SOURCES = {"FeatureScalingConfig", "SpacetimeFeatureEngine", "LocalFeatureJob", "PilotExportJob", "WindowArtifactInput"}
FEATURE_TESTS = {"FeatureScalingConfigTest", "SpacetimeFeatureEngineTest", "PilotExportJobTest", "FeatureParquetCrossProcessTest"}
PILOT_SCRIPTS = {"scripts/local-ml.py", "scripts/stage-ml-windows.py", "scripts/inspect-pilot-notebook.py", "scripts/pack-pilot.py"}
PILOT_CONFIGS = {"config/ml/feature_scaling_global_dev_v1.json", "config/ml/feature_scaling_adaptive_dev_v1.json", "config/ml/dat02_reproduction_v1.json"}
PILOT_FILES = {"notebooks/pilot_bundle.py", "notebooks/dat_02_inspection.ipynb"}
LEAF_DAGS = {"airflow/dags/" + name + ".py" for name in
             ("afl_01_smoke", "usg_04_usgs_ingest", "jma_04_year_backfill", "orc_01_etl_pipeline", "orc_02_daily_sources", "orc_03_backfill")}


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, check=True, timeout=30).stdout


def full(reason, count=0):
    return {"policy_version": "impact-v1", "java": "full", "airflow": "full", "pilot": "full", "java_tests": [],
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
        if path in PILOT_FILES or path in PILOT_SCRIPTS or re.fullmatch(r"tests/test_pilot_[a-z0-9_]+\.py", path):
            pilot = "full"; reasons.add("pilot-python")
            # Runtime scripts orchestrate the Java feature/export boundary too.
            if path in PILOT_SCRIPTS: scopes.add("feature")
            continue
        if path in PILOT_CONFIGS:
            scopes.add("feature"); pilot = "full"; reasons.add("feature-config"); continue
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
        if "ml" not in scopes: paths = [p for p in paths if p.stem in FEATURE_TESTS]
        if not paths or not FEATURE_TESTS.issubset({p.stem for p in paths}): return full("missing-selected-test-coverage", len(changes))
        for path in paths:
            name = path.relative_to(root / "spark/src/test/java").with_suffix("").as_posix().replace("/", ".")
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+", name):
                return full("invalid-test-selector", len(changes))
            tests.append(name)
    return {"policy_version": "impact-v1", "java": "selected" if tests else "none", "airflow": airflow, "pilot": pilot,
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


def emit(result, output=None, summary=None):
    print(json.dumps(result, sort_keys=True), flush=True)
    if output:
        # Outputs contain fixed enums only, never PR file names, titles or executable strings.
        with Path(output).open("a") as file:
            for component in ("java", "airflow", "pilot"): file.write(component + "=" + result[component] + "\n")
    if summary:
        with Path(summary).open("a") as file:
            file.write("### Impact-based CI\n\nDocs/contracts/security: always. "
                       f"Java: **{result['java']}** ({len(result['java_tests'])} selected classes); "
                       f"Airflow: **{result['airflow']}**; pilot: **{result['pilot']}**.\n\n"
                       f"Changed paths: {result['change_count']}; reasons: {', '.join(result['reasons'])}.\n\n"
                       "`none` means not applicable to this comparison, not a suppressed failure.\n")


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["plan", "run"])
    parser.add_argument("--event", default=os.environ.get("CI_EVENT_NAME", "local"))
    parser.add_argument("--base", default=os.environ.get("CI_BASE_SHA", ""))
    parser.add_argument("--head", default=os.environ.get("CI_HEAD_SHA", "HEAD"))
    parser.add_argument("--force-full", action="store_true")
    parser.add_argument("--component", choices=["java", "airflow", "pilot"])
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--summary", type=Path)
    args = parser.parse_args()
    if args.action == "run" and not args.component: parser.error("run requires --component")
    result = plan(args.event, args.base, args.head, force_full=args.force_full)
    emit(result, args.github_output, args.summary)
    if args.action == "run": execute(result, args.component)


if __name__ == "__main__": main()
