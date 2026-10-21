"""Opt-in runtime acceptance. Python controls ownership; Java owns all ETL/data reads.

No lifecycle, cleanup, fixture-to-PASS fallback, or automatic takeover on timeout.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import resource
import uuid

from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity
from source_run_guard import lease
import qa_02_rerun_revision as qa02
import real_etl_runtime as etl

STAGING = Path("/opt/pipeline/staging")
JOBS = {"QA-02": "ie212.earthquake.spark.usgs.RerunRevisionAcceptanceJob"}
GOLD_BLOCKER_CHECKS = {
    "required_fields", "bridge_lineage", "source_counts", "primary_provenance",
    "utc_jst", "dimensions", "aggregate_count", "changed_run_scope",
    "outside:event_current", "outside:event_source_bridge",
} | {"unique:" + name for name in etl.TABLES}


def validate_publication(record, namespace, root):
    context = record["context"]
    etl.validate_context(context)
    if context["resolver_request"]["namespace"] != namespace or record["operation_id"] != context["operation_id"]:
        raise RuntimeError("PIPELINE_QA_PUBLICATION_IDENTITY_CHANGED")
    receipts = record.get("receipts", [])
    if len(receipts) != len(etl.PHASES):
        raise RuntimeError("PIPELINE_QA_PHASES_INCOMPLETE")
    prior = None
    artifacts = {}
    for phase, receipt in zip(etl.PHASES, receipts):
        etl.validate_receipt(phase, context, prior, receipt)
        path = root / context["operation_id"] / (phase + ".json")
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 4 * 1024 * 1024:
            raise RuntimeError("PIPELINE_QA_ARTIFACT_MISSING")
        artifact = json.loads(path.read_text())
        if etl.digest(artifact) != receipt["artifact_sha256"]:
            raise RuntimeError("PIPELINE_QA_ARTIFACT_CHANGED")
        artifacts[phase] = artifact
        prior = receipt
    commit, publication = record["commit"], record["publication"]
    if (commit.get("namespace") != namespace or commit.get("gold_run_id") != context["operation_id"]
            or set(commit.get("tables", {})) != etl.TABLES or commit.get("status") != "COMMITTED"
            or publication.get("engine") != "TRINO" or publication.get("verify_status") != "PASSED"
            or publication.get("published") is not True
            or publication.get("gold_run_id") != context["operation_id"]
            or publication.get("namespace") != namespace
            or publication.get("identity_sha256") != commit.get("identity_sha256")
            or publication.get("snapshot_bundle") != commit["tables"]):
        raise RuntimeError("PIPELINE_QA_PUBLICATION_INVALID")
    checks = publication.get("checks", {})
    required_checks = set(GOLD_BLOCKER_CHECKS) | {kind + ":" + name for name in etl.TABLES for kind in ("count", "identity")}
    commit_sha = hashlib.sha256(json.dumps(commit, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    if (not required_checks <= set(checks) or commit.get("published") is not False
            or publication.get("bundle_sha256") != commit_sha
            or publication.get("publication_id") != "pub_" + commit_sha):
        raise RuntimeError("PIPELINE_QA_PUBLICATION_PIN_OR_CHECKS_MISSING")
    for name, pin in commit["tables"].items():
        if (type(pin.get("snapshot_id")) is not int or not 0 < pin["snapshot_id"] <= 9223372036854775807
                or type(pin.get("count")) is not int or pin["count"] < 0
                or pin.get("table") != namespace + "." + name
                or checks.get("count:" + name, {}).get("actual") != pin["count"]
                or checks.get("count:" + name, {}).get("expected") != pin["count"]
                or checks.get("identity:" + name, {}).get("actual") != 1
                or checks.get("identity:" + name, {}).get("expected") != 1):
            raise RuntimeError("PIPELINE_QA_TRINO_READBACK_INVALID")
    if record["silver"].get("reconciliation", {}).get("balanced") is not True:
        raise RuntimeError("PIPELINE_QA_COUNTS_UNBALANCED")
    if any(not isinstance(check, dict) or type(check.get("actual")) is not int
           or type(check.get("expected")) is not int or check["actual"] != check["expected"] for check in checks.values()):
        raise RuntimeError("PIPELINE_QA_TRINO_BLOCKER_FAILED")
    if any(checks[name]["expected"] != 0 for name in GOLD_BLOCKER_CHECKS):
        raise RuntimeError("PIPELINE_QA_TRINO_BLOCKER_EXPECTATION_CHANGED")
    for phase, field in (("silver", "silver"), ("gold", "commit"), ("publish", "publication")):
        if artifacts[phase] != record[field]:
            raise RuntimeError("PIPELINE_QA_RECORD_ARTIFACT_CHANGED")
    expected_snapshots = {name: pin["snapshot_id"] for name, pin in commit["tables"].items()}
    if any(receipt["snapshots"] != expected_snapshots for receipt in receipts[3:]):
        raise RuntimeError("PIPELINE_QA_RECEIPT_SNAPSHOT_CHANGED")
    if receipts[-1]["publication_id"] != publication["publication_id"]:
        raise RuntimeError("PIPELINE_QA_RECEIPT_PUBLICATION_CHANGED")


def validate(task, request, report):
    if task == "QA-02":
        # The request's non-existing output path is checked BEFORE launch.
        # After execution it must exist; reusing the pre-run guard rejects every success.
        matrix_path = qa02._safe_path(request["matrix_path"], "QA02_MATRIX_REFERENCE_INVALID", must_exist=True)
        if qa02._sha256(matrix_path) != request["matrix_sha256"]:
            raise RuntimeError("QA02_MATRIX_CHANGED_AFTER_EXECUTION")
        matrix = qa02.load_matrix(matrix_path)
        path = qa02._safe_path(request["report_path"], "QA02_REPORT_REFERENCE_INVALID", must_exist=True)
        qa02._validate_runtime_report(report, request, matrix, path)
        # The generic report validator checks assertions and evidence identities.
        # This fixed runner additionally requires actual six-phase publications.
        for case in report["cases"]:
            evidence = json.loads(Path(case["evidence_ref"]["path"]).read_text())
            if evidence.get("reconciliation") != case["reconciliation"]:
                raise RuntimeError("QA02_EVIDENCE_RECONCILIATION_CHANGED")
            for side in ("before", "after"):
                record = evidence[side]
                validate_publication(record, request["namespace"], path.parent)
                expected = {(pin["table"], str(pin["snapshot_id"])) for pin in record["commit"]["tables"].values()}
                actual = {(pin["table"], str(pin["snapshot_id"])) for pin in case["snapshots"][side]}
                if not expected <= actual:
                    raise RuntimeError("QA02_EVIDENCE_SNAPSHOT_PINS_CHANGED")
    else:
        raise ValueError("UNSUPPORTED_PIPELINE_QA_TASK")


def run(task, environment=None):
    if task not in JOBS:
        raise ValueError("UNSUPPORTED_PIPELINE_QA_TASK")
    env = dict(os.environ if environment is None else environment)
    tag = task.lower().replace("-", "")
    run_id = tag + "-runtime-" + uuid.uuid4().hex
    owner = {"dag_id": "pipeline_qa_acceptance", "run_id": run_id}
    root = STAGING / task.lower() / run_id
    namespace = env["ICEBERG_CATALOG_NAME"] + "." + tag + "_" + uuid.uuid4().hex
    matrix_name = "qa_02_rerun_revision_matrix.json"
    matrix = json.loads((Path(__file__).parent / "fixtures" / matrix_name).read_text())
    matrix["input_pins"]["namespace"] = namespace
    _atomic_json(root / "matrix.json", matrix)
    request = {"task_id": task, "request_version": "qa-02-runtime-v1", "run_id": run_id,
               "operation_id": run_id, "namespace": namespace, "lease_owner": owner,
               "matrix_path": str(root / "matrix.json"),
               "matrix_sha256": hashlib.sha256((root / "matrix.json").read_bytes()).hexdigest(),
               "report_path": str(root / "runtime-report.json")}
    _atomic_json(root / "request.json", request)
    print("event=pipeline_qa_started task=" + task + " evidence_root=" + str(root), flush=True)
    qa02._validate_runtime_request(request)
    if lease("acquire", owner, env) != {"status": "ACQUIRED", **owner}:
        raise RuntimeError("PIPELINE_QA_LEASE_UNCONFIRMED")
    unknown = False
    started = time.monotonic()
    try:
        java_root = root / "java-identity"
        java_root.mkdir()
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m",
                   "--conf", "spark.driver.host=127.0.0.1", "--conf", "spark.driver.bindAddress=127.0.0.1",
                   "--conf", "spark.ui.enabled=false", "--jars",
                   "/opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar",
                   "--class", JOBS[task], "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(root / "request.json")]
        with (root / "stdout.private.log").open("wb") as output, (root / "stderr.private.log").open("wb") as error:
            os.chmod(output.name, 0o600)
            os.chmod(error.name, 0o600)
            unknown = True
            try:
                process = subprocess.run(command, env=java_identity(env, java_root), stdout=output, stderr=error,
                                         timeout=3600, check=False)
            except subprocess.TimeoutExpired:
                _atomic_json(root / "failure.json", {"status": "TIMEOUT_LEASE_RETAINED", "owner": owner})
                raise RuntimeError("PIPELINE_QA_TIMEOUT_CHECK_PROCESS_BEFORE_RELEASE") from None
            unknown = False
        _atomic_json(root / "execution.json", {"job_class": JOBS[task], "exit_code": process.returncode,
                                               "request_sha256": hashlib.sha256((root / "request.json").read_bytes()).hexdigest()})
        if process.returncode != 0:
            # A JVM exit alone cannot prove a remote Trino write has quiesced.
            unknown = True
            _atomic_json(root / "failure.json", {"status": "FAILED_LEASE_RETAINED", "exit_code": process.returncode})
            raise RuntimeError("PIPELINE_QA_FAILED_CHECK_SCOPED_PRIVATE_LOG")
        report = json.loads((root / "runtime-report.json").read_text())
        validate(task, request, report)
        result = {"task": task, "status": "PASS", "namespace": namespace, "run_id": run_id,
                  "report_path": str(root / "runtime-report.json"),
                  "report_sha256": hashlib.sha256((root / "runtime-report.json").read_bytes()).hexdigest(),
                  "summary": report["summary"],
                  "capacity": {"duration_seconds": time.monotonic() - started,
                               "child_peak_rss_mb": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024,
                               "profile": "local[1], driver heap 768m, container limit 2GiB"}}
    finally:
        # Only a successful fixed job (all remote queries returned) or a failure
        # before launch is known quiescent; timeout/nonzero keeps the exact owner.
        if not unknown:
            if lease("assert", owner, env) != {"status": "HELD", **owner}:
                raise RuntimeError("PIPELINE_QA_LEASE_CHANGED")
            released = lease("release", owner, env)
            if released != {"status": "RELEASED", **owner}:
                raise RuntimeError("PIPELINE_QA_RELEASE_UNCONFIRMED")
            _atomic_json(root / "lease-release.json", released)
            if "result" in locals():
                result["lease_release"] = released
                _atomic_json(root / "acceptance.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", choices=JOBS, required=True)
    try:
        print(json.dumps(run(parser.parse_args().task), sort_keys=True))
    except Exception:
        raise SystemExit("PIPELINE_QA_FAILED_CHECK_SCOPED_STAGING") from None
