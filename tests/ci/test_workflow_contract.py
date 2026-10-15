"""Offline guardrails for CI selection, task suites and required gates; not actionlint."""
from copy import deepcopy
from pathlib import Path
import re
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github/workflows/ci.yml"
PLAN = 'python3 scripts/ci-impact.py plan --github-output "$GITHUB_OUTPUT" --summary "$GITHUB_STEP_SUMMARY"'
GATE = "python3 scripts/ci-impact.py gate"


class UniqueKeyLoader(yaml.BaseLoader):
    """Preserve 'on' and reject duplicate YAML keys."""
    def construct_mapping(self, node, deep=False):
        mapping = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping: raise ValueError(f"Duplicate YAML key: {key}")
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


def validate_workflow(workflow):
    def require(condition, message):
        if not condition: raise ValueError(message)

    require(workflow["name"] == "Project CI", "stable workflow name")
    events = workflow["on"]
    require(set(events) == {"pull_request", "push", "workflow_dispatch"}, "safe events")
    require(events["pull_request"] in ("", None, {}), "unfiltered stacked PRs")
    require(events["push"] == {"branches": ["main"]}, "main-only pushes")
    require(workflow["permissions"] == {"contents": "read"}, "read-only token")
    require(workflow["concurrency"]["cancel-in-progress"] == "true", "cancel stale runs")
    require("github.event_name" in workflow["concurrency"]["group"], "separate PR/main")
    for key, value in {"CI_EVENT_NAME": "${{ github.event_name }}",
                       "CI_BASE_SHA": "${{ github.event.pull_request.base.sha || github.event.before }}",
                       "CI_HEAD_SHA": "${{ github.sha }}"}.items():
        require(workflow["env"][key] == value, "trusted comparison context")
    jobs = workflow["jobs"]
    require(set(jobs) == {"impact", "docs-contracts", "java-tests", "airflow-tests", "pilot", "java-spark", "airflow"}, "reviewed graph")
    names = {"impact": "impact-plan", "java-tests": "Java / ${{ matrix.suite }}",
             "airflow-tests": "Control / ${{ matrix.suite }}", "pilot": "DAT-02 / pilot-handoff"}
    dependencies = {"docs-contracts": ["impact", "pilot"], "java-spark": ["impact", "java-tests"],
                    "airflow": ["impact", "airflow-tests"], "java-tests": "impact", "airflow-tests": "impact", "pilot": "impact"}
    conditions = {name: "${{ always() }}" for name in ("docs-contracts", "java-spark", "airflow")}
    conditions.update({name: "${{ needs.impact.outputs." + component + " != 'none' }}"
                       for name, component in (("java-tests", "java"), ("airflow-tests", "airflow"), ("pilot", "pilot"))})
    targets = {"impact": PLAN, "docs-contracts": "make ci-docs-base", "java-tests": "make ci-suite",
               "airflow-tests": "make ci-suite", "pilot": "make ci-pilot-impact", "java-spark": GATE, "airflow": GATE}
    for name, job in jobs.items():
        require(job["name"] == names.get(name, name), "stable/descriptive check names")
        require(job["runs-on"] == "ubuntu-24.04", "isolated runner")
        require(0 < int(job["timeout-minutes"]) <= 30, "bounded timeout")
        require(not {"continue-on-error", "permissions", "environment"} & set(job), "no bypass")
        require(job.get("needs") == dependencies.get(name), "reviewed dependencies")
        require(job.get("if") == conditions.get(name), "mandatory gates and reviewed skips")
        steps = job["steps"]
        require(any(step.get("run") == targets[name] and "if" not in step for step in steps), "mandatory test/guard command")
        checkouts = []
        for step in steps:
            require("continue-on-error" not in step, "never suppress failure")
            if "run" in step:
                require("if" not in step, "never skip mandatory step")
                require(not re.search(r"\|\|\s*true|secrets\.|pull_request_target|make\s+(?:up|smoke)|docker\s+compose\s+(?:up|run)", step["run"]), "no live writes/suppression")
                require("${{" not in step["run"], "context values through quoted environment only")
            if "uses" in step:
                require(re.fullmatch(r"actions/[a-z-]+@[a-f0-9]{40}", step["uses"]) is not None, "pin official action SHA")
                if step["uses"].startswith("actions/checkout@"):
                    checkouts.append(step)
                    require(step["with"].get("fetch-depth") == "0", "complete diff history")
                    require(step["with"].get("persist-credentials") == "false", "do not persist token")
                if not step["uses"].startswith("actions/upload-artifact@"):
                    require("if" not in step, "required setup")
        require(len(checkouts) == 1, "single checkout")
    require(jobs["impact"]["outputs"] == {key: "${{ steps.impact.outputs." + key + " }}"
            for key in ("java", "airflow", "pilot", "java_matrix", "airflow_matrix")}, "trusted planner outputs")
    require(next(s for s in jobs["impact"]["steps"] if s.get("run") == PLAN).get("id") == "impact", "planner output step")
    for name, component, worker in (("docs-contracts", "pilot", "pilot"), ("java-spark", "java", "java-tests"),
                                    ("airflow", "airflow", "airflow-tests")):
        gates = [step for step in jobs[name]["steps"] if step.get("run") == GATE]
        require(len(gates) == 1 and gates[0].get("env") == {
            "CI_PLANNER_RESULT": "${{ needs.impact.result }}", "CI_MODE": "${{ needs.impact.outputs." + component + " }}",
            "CI_SUITE_RESULT": "${{ needs." + worker + ".result }}"}, "gate checks planner plus worker result")
    for name, component, parallel in (("java-tests", "java", "3"), ("airflow-tests", "airflow", "4")):
        require(jobs[name]["strategy"] == {"fail-fast": "false", "max-parallel": parallel,
                "matrix": "${{ fromJSON(needs.impact.outputs." + component + "_matrix) }}"}, "selected bounded matrix; no cancellation masking")
        expected = {"CI_COMPONENT": component, "CI_SUITE": "${{ matrix.suite }}"}
        if component == "java": expected.update(SPARK_LOCAL_IP="127.0.0.1", SPARK_LOCAL_HOSTNAME="localhost")
        require(jobs[name]["env"] == expected, "safe suite args/loopback")
    java = next(s for s in jobs["java-tests"]["steps"] if s.get("uses", "").startswith("actions/setup-java@"))
    require(java["with"]["java-version"] == "17", "supported Java")
    reports = next(s for s in jobs["java-tests"]["steps"] if s.get("uses", "").startswith("actions/upload-artifact@"))
    require(reports.get("if") == "${{ always() }}", "reports on failure")
    require(reports["with"] == {"name": "java-test-reports-${{ matrix.suite }}", "path": "spark/target/surefire-reports/*.xml",
                                "if-no-files-found": "warn", "retention-days": "7"}, "isolated bounded test artifacts only")
    require(any("actionlint\" .github/workflows/ci.yml" in s.get("run", "") for s in jobs["docs-contracts"]["steps"]), "Actions lint")
    require(any('git diff --check "$CI_BASE_SHA" "$CI_HEAD_SHA"' in s.get("run", "") for s in jobs["docs-contracts"]["steps"]), "committed diff whitespace")
    require("secrets." not in str(workflow), "no CI secrets")


class WorkflowContractTest(unittest.TestCase):
    def setUp(self):
        self.workflow = yaml.load(WORKFLOW.read_text(), Loader=UniqueKeyLoader)

    def test_checked_in_workflow(self): validate_workflow(self.workflow)

    def test_pull_request_has_no_base_or_path_filter(self):
        self.assertIn(self.workflow["on"]["pull_request"], ("", None, {}))
        self.assertEqual(self.workflow["on"]["push"], {"branches": ["main"]})

    def test_duplicate_yaml_key_rejected(self):
        with self.assertRaisesRegex(ValueError, "Duplicate YAML key"):
            yaml.load("name: first\nname: second\n", Loader=UniqueKeyLoader)

    def test_unsafe_or_skipped_configuration_rejected(self):
        def step(w, job, run): return next(s for s in w["jobs"][job]["steps"] if s.get("run") == run)
        mutations = [
            lambda w: w["permissions"].update(contents="write"),
            lambda w: w["on"].update(pull_request_target={}),
            lambda w: w["on"].update(pull_request={"paths": ["spark/**"]}),
            lambda w: w["on"].update(pull_request={"paths-ignore": ["docs/**"]}),
            lambda w: w["on"].update(pull_request={"branches": ["main"]}),
            lambda w: w["on"].update(pull_request={"branches-ignore": ["feat/**"]}),
            lambda w: w["on"].update(pull_request={"types": ["opened"]}),
            lambda w: w["on"].update(push={}),
            lambda w: w["on"].update(push={"branches": ["main", "feat/**"]}),
            lambda w: w["jobs"]["airflow-tests"].update(**{"continue-on-error": "true"}),
            lambda w: w["jobs"]["airflow"]["steps"][0].update(uses="actions/checkout@main"),
            lambda w: w["jobs"]["airflow"]["steps"][0]["with"].update(**{"persist-credentials": "true"}),
            lambda w: step(w, "airflow-tests", "make ci-suite").update(run="make ci-airflow || true"),
            lambda w: step(w, "airflow-tests", "make ci-suite").update(run="make smoke-usgs-live"),
            lambda w: w["env"].update(CI_EVENT_NAME="pull_request"),
            lambda w: w["jobs"]["impact"]["steps"][0]["with"].update(**{"fetch-depth": "1"}),
            lambda w: step(w, "docs-contracts", "make ci-docs-base").update(**{"if": "false"}),
            lambda w: step(w, "impact", PLAN).update(**{"if": "false"}),
            lambda w: w["jobs"]["java-tests"]["strategy"].update(**{"fail-fast": "true"}),
            lambda w: w["jobs"]["java-tests"]["strategy"].update(matrix={"suite": ["mld-04-features"]}),
            lambda w: w["jobs"]["pilot"].update(needs="java-tests"),
            lambda w: w["jobs"]["impact"]["outputs"].update(java="none"),
        ]
        for job in ("docs-contracts", "java-spark", "airflow"):
            mutations.extend([
                lambda w, job=job: w["jobs"][job].update(**{"if": "${{ success() }}"}),
                lambda w, job=job: w["jobs"][job].update(needs="impact"),
                lambda w, job=job: step(w, job, GATE).update(**{"if": "false"}),
                lambda w, job=job: step(w, job, GATE)["env"].update(CI_SUITE_RESULT="success"),
                lambda w, job=job: step(w, job, GATE)["env"].update(CI_PLANNER_RESULT="success"),
            ])
        for index, mutate in enumerate(mutations):
            with self.subTest(mutation=index):
                workflow = deepcopy(self.workflow)
                mutate(workflow)
                with self.assertRaises(ValueError): validate_workflow(workflow)


if __name__ == "__main__": unittest.main()
