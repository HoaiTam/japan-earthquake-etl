"""Offline guardrails for the CI configuration; not a replacement for actionlint."""

from copy import deepcopy
from pathlib import Path
import re
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/ci.yml"


class UniqueKeyLoader(yaml.BaseLoader):
    """Preserve 'on' as a string and fail rather than overwrite duplicate keys."""

    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise ValueError(f"Duplicate YAML key: {key}")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def validate_workflow(workflow):
    def require(condition, message):
        if not condition:
            raise ValueError(message)

    require(workflow["name"] == "Project CI", "stable workflow name")
    events = workflow["on"]
    require(set(events) == {"pull_request", "push", "workflow_dispatch"}, "safe events")
    require(events["pull_request"] in ("", None, {}), "unfiltered PRs, including stacked feature branches")
    require(events["push"] == {"branches": ["main"]}, "main-only pushes avoid duplicate feature checks")
    require(workflow["permissions"] == {"contents": "read"}, "read-only token")
    require(workflow["concurrency"]["cancel-in-progress"] == "true", "cancel stale runs")
    require("github.event_name" in workflow["concurrency"]["group"], "separate PR and main runs")
    for key, value in {"CI_EVENT_NAME": "${{ github.event_name }}",
                       "CI_BASE_SHA": "${{ github.event.pull_request.base.sha || github.event.before }}",
                       "CI_HEAD_SHA": "${{ github.sha }}"}.items():
        require(workflow["env"][key] == value, "trusted comparison context")
    jobs = workflow["jobs"]
    require(set(jobs) == {"docs-contracts", "java-spark", "airflow"}, "three stable checks")
    require(jobs["java-spark"]["env"] == {"SPARK_LOCAL_IP": "127.0.0.1", "SPARK_LOCAL_HOSTNAME": "localhost"}, "loopback-only Spark tests")
    targets = {"docs-contracts": "ci-docs-base", "java-spark": "ci-java-impact", "airflow": "ci-airflow-impact"}
    conditions = {"make ci-pilot-impact": "${{ steps.impact.outputs.pilot != 'none' }}",
                  "make ci-java-impact": "${{ steps.impact.outputs.java != 'none' }}",
                  "make ci-airflow-impact": "${{ steps.impact.outputs.airflow != 'none' }}"}
    for name, job in jobs.items():
        require(job["name"] == name, "stable check name")
        require(job["runs-on"] == "ubuntu-24.04", "isolated hosted runner")
        require(0 < int(job["timeout-minutes"]) <= 30, "bounded timeout")
        require(not {"if", "needs", "continue-on-error", "permissions", "environment"} & set(job), "independent mandatory jobs")
        steps = job["steps"]
        require(any(step.get("run") == f"make {targets[name]}" for step in steps), "shared local test command")
        impact = [step for step in steps if step.get("id") == "impact"]
        require(len(impact) == 1 and impact[0].get("run") == 'python3 scripts/ci-impact.py plan --github-output "$GITHUB_OUTPUT" --summary "$GITHUB_STEP_SUMMARY"'
                and "if" not in impact[0], "mandatory local diff planner")
        require(steps.index(impact[0]) < next(i for i, step in enumerate(steps) if step.get("run") == f"make {targets[name]}"), "plan before tests")
        checkout = []
        for step in steps:
            require("continue-on-error" not in step, "never hide a failing check")
            if "run" in step:
                require(step.get("if") == conditions.get(step["run"]), "only reviewed impact-based conditions")
                require(not re.search(r"\|\|\s*true|secrets\.|pull_request_target|make\s+(?:up|smoke)|docker\s+compose\s+(?:up|run)", step["run"]), "no suppression, secrets or live pipeline")
            if "uses" in step:
                require(re.fullmatch(r"actions/[a-z-]+@[a-f0-9]{40}", step["uses"]) is not None, "pin official actions to full SHA")
                if step["uses"].startswith("actions/checkout@"):
                    checkout.append(step)
                    require(step["with"].get("fetch-depth") == "0", "complete local diff, no API pagination limit")
                elif step["uses"].startswith("actions/setup-java@"):
                    require(step.get("if") == conditions["make ci-java-impact"], "prepare Java only when applicable")
                elif step["uses"].startswith("actions/setup-python@"):
                    require(step.get("if") == (conditions["make ci-airflow-impact"] if name == "airflow" else None), "safe Python setup condition")
        require(len(checkout) == 1 and checkout[0]["with"]["persist-credentials"] == "false", "do not persist checkout token")
    java = next(step for step in jobs["java-spark"]["steps"] if step.get("uses", "").startswith("actions/setup-java@"))
    require(java["with"]["java-version"] == "17", "test on supported Java runtime")
    reports = next(step for step in jobs["java-spark"]["steps"] if step.get("uses", "").startswith("actions/upload-artifact@"))
    require(reports.get("if") == "${{ always() && steps.impact.outputs.java != 'none' }}", "reports on applicable test failure")
    require(reports["with"]["path"] == "spark/target/surefire-reports/*.xml", "upload test reports only")
    require(reports["with"]["retention-days"] == "7", "bounded artifact retention")
    require(any("actionlint\" .github/workflows/ci.yml" in step.get("run", "") for step in jobs["docs-contracts"]["steps"]), "lint Actions syntax")
    docs_checkout = jobs["docs-contracts"]["steps"][0]
    require(docs_checkout["with"]["fetch-depth"] == "0", "fetch comparison history")
    require(any('git diff --check "$CI_BASE_SHA" "$CI_HEAD_SHA"' in step.get("run", "") for step in jobs["docs-contracts"]["steps"]), "check committed changes, not just clean checkout")
    require("secrets." not in str(workflow), "no secrets in offline CI")
    require(any(step.get("run") == "make ci-pilot-impact" for step in jobs["docs-contracts"]["steps"]), "pilot consumers remain checked")


class WorkflowContractTest(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load(WORKFLOW.read_text(), Loader=UniqueKeyLoader)

    def test_checked_in_workflow(self):
        validate_workflow(self.workflow)

    def test_pull_request_has_no_base_or_path_filter(self):
        self.assertIn(self.workflow["on"]["pull_request"], ("", None, {}))
        self.assertEqual(self.workflow["on"]["push"], {"branches": ["main"]})

    def test_duplicate_yaml_key_rejected(self):
        with self.assertRaisesRegex(ValueError, "Duplicate YAML key"):
            yaml.load("name: first\nname: second\n", Loader=UniqueKeyLoader)

    def test_unsafe_or_skipped_configuration_rejected(self):
        mutations = (
            lambda w: w["permissions"].update(contents="write"),
            lambda w: w["on"].update(pull_request_target={}),
            lambda w: w["on"].update(pull_request={"paths": ["spark/**"]}),
            lambda w: w["on"].update(pull_request={"paths-ignore": ["docs/**"]}),
            lambda w: w["on"].update(pull_request={"branches": ["main"]}),
            lambda w: w["on"].update(pull_request={"branches-ignore": ["feat/**"]}),
            lambda w: w["on"].update(pull_request={"types": ["opened"]}),
            lambda w: w["on"].update(push={}),
            lambda w: w["on"].update(push={"branches": ["main", "feat/**"]}),
            lambda w: w["jobs"]["airflow"].update(**{"continue-on-error": "true"}),
            lambda w: w["jobs"]["airflow"].update(**{"if": "false"}),
            lambda w: w["jobs"]["airflow"].update(needs="java-spark"),
            lambda w: w["jobs"]["airflow"]["steps"][0].update(uses="actions/checkout@main"),
            lambda w: w["jobs"]["airflow"]["steps"][0]["with"].update(**{"persist-credentials": "true"}),
            lambda w: w["jobs"]["airflow"]["steps"][-1].update(run="make ci-airflow || true"),
            lambda w: w["jobs"]["airflow"]["steps"][-1].update(run="make smoke-usgs-live"),
            lambda w: w["env"].update(CI_EVENT_NAME="pull_request"),
            lambda w: w["jobs"]["java-spark"]["steps"][1].update(**{"if": "false"}),
            lambda w: w["jobs"]["java-spark"]["steps"][0]["with"].update(**{"fetch-depth": "1"}),
            lambda w: next(s for s in w["jobs"]["docs-contracts"]["steps"] if s.get("run") == "make ci-docs-base").update(**{"if": "false"}),
            lambda w: w["jobs"]["java-spark"]["steps"][-2].update(**{"if": "${{ false }}"}),
        )
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                workflow = deepcopy(self.workflow)
                mutate(workflow)
                with self.assertRaises(ValueError):
                    validate_workflow(workflow)


if __name__ == "__main__":
    unittest.main()
