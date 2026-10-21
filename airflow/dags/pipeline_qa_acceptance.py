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
import re
import uuid

from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity
from source_run_guard import lease
import qa_02_rerun_revision as qa02
import qa_03_jma_historical_backfill as qa03
import qa_04_storage_acceptance as qa04
import real_etl_runtime as etl

STAGING = Path("/opt/pipeline/staging")
CAPTURES = Path("/opt/pipeline/qa-history-captures")
INVENTORY = Path("/opt/pipeline/config/jma/hypocenter_archives_v1.csv")
JOBS = {"QA-02": "ie212.earthquake.spark.usgs.RerunRevisionAcceptanceJob",
        "QA-03": "ie212.earthquake.spark.usgs.HistoricalBackfillAcceptanceJob",
        "QA-04": "ie212.earthquake.spark.usgs.FailureRecoveryAcceptanceJob"}
HISTORY_FILES = {"h199701.zip", "h199710.zip", "h2000.zip", "h2023.zip", "usgs-2023-09.geojson"}
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
    # GoldPublisher hashes Jackson's ordered commit bytes, while RealEtlAdapter
    # canonicalizes phase artifacts and Python sorts the final QA-04 report.
    # The per-operation Java evidence preserves the original commit field order.
    # Bind its content to BOTH the report and receipt-verified phase artifact.
    original_path = root / context["operation_id"] / "acceptance.json"
    if original_path.is_symlink() or not original_path.is_file() or original_path.stat().st_size > 4 * 1024 * 1024:
        raise RuntimeError("PIPELINE_QA_ORIGINAL_COMMIT_MISSING")
    original = json.loads(original_path.read_text()).get("commit")
    if original != commit or original != artifacts["gold"]:
        raise RuntimeError("PIPELINE_QA_ORIGINAL_COMMIT_CHANGED")
    commit_sha = hashlib.sha256(json.dumps(original, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
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


def history_pins():
    """Only byte identity/size checks; all archive/event parsing belongs to Java."""
    path = CAPTURES / "capture-pins.json"
    pins = qa03._load(path, 8192, "CAPTURE_PINS")
    if set(pins) != HISTORY_FILES:
        raise RuntimeError("QA03_CAPTURE_SET_CHANGED")
    for name, checksum in pins.items():
        file = qa03._safe_path(str(CAPTURES / name), "QA03_UNSAFE_CAPTURE", must_exist=True)
        if (not qa03.SAFE_SHA256.fullmatch(str(checksum)) or not 0 < file.stat().st_size <= 32 * 1024 * 1024
                or qa03._sha256_file(file, "QA03_CAPTURE_UNREADABLE") != checksum):
            raise RuntimeError("QA03_CAPTURE_PIN_CHANGED")
    return pins, qa03._sha256_file(path, "QA03_CAPTURE_PINS_UNREADABLE")


def validate_history(request, report):
    matrix_file = qa03._safe_path(request["matrix_path"], "QA03_MATRIX_UNSAFE", must_exist=True)
    matrix = qa03.load_matrix(matrix_file, INVENTORY)
    pins, capture_sha = history_pins()
    if (matrix["_matrix_sha256"] != request["matrix_sha256"]
            or matrix["input_pins"]["namespace"] != request["namespace"]
            or report.get("matrix_sha256") != request["matrix_sha256"]
            or matrix["inventory_sha256"] != request["inventory_sha256"]
            or report.get("inventory_sha256") != request["inventory_sha256"]
            or capture_sha != request["capture_pins_sha256"] or report.get("capture_pins_sha256") != capture_sha
            or report.get("capture_sha256") != pins):
        raise RuntimeError("QA03_RUNTIME_PINS_CHANGED")
    if (report.get("schema_version") != "qa03-storage-acceptance-v1" or report.get("task") != "QA-03"
            or report.get("namespace") != request["namespace"] or report.get("run_id") != request["run_id"]
            or report.get("status") != "PASS" or report.get("original_public_captures") is not True
            or report.get("controlled_release_is_synthetic") is not True or report.get("research_ready") is not False
            or type(report.get("download_count_during_execution")) is not int or report["download_count_during_execution"] != 0
            or report.get("checks") != {name: True for name in qa03.REQUIRED_CASES}
            or any(type(flag) is not bool for flag in report["checks"].values())
            or report.get("summary") != {"case_count": 6, "passed": 6, "failed": 0, "pending_runtime": 0}):
        raise RuntimeError("QA03_RUNTIME_REPORT_INVALID")
    qa03._utc_instant(report.get("source_cutoff_utc"))
    completed = report.get("completed_at_utc")
    if not isinstance(completed, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z", completed):
        raise RuntimeError("QA03_COMPLETION_INSTANT_INVALID")
    # Java Instant emits nanoseconds; Python 3.8 in the Spark image only
    # accepts up to microseconds. Preserve the report bytes, normalize ONLY
    # the parsed metadata copy, never event time or the pinned source cutoff.
    qa03._utc_instant(re.sub(r"(\.\d{6})\d+Z$", r"\1Z", completed))
    root = qa03._safe_path(request["report_path"], "QA03_OUTPUT_MISSING", must_exist=True).parent
    records = report.get("publications", [])
    expected_months = [["2000-02"], ["1997-09"], ["1997-10"], ["2000-01"], ["2023-09"], ["1997-09"]]
    if len(records) != 6 or len({r["operation_id"] for r in records}) != 6:
        raise RuntimeError("QA03_PUBLICATIONS_INCOMPLETE")
    for record, months in zip(records, expected_months):
        validate_publication(record, request["namespace"], root)
        resolver = record["context"]["resolver_request"]
        if (resolver["target_months"] != months or record["silver"]["affected_months"] != months
                or resolver["source_cutoff_utc"] != report["source_cutoff_utc"]):
            raise RuntimeError("QA03_PUBLICATION_SCOPE_CHANGED")
    source = report["bronze_inputs"]
    if set(source) != HISTORY_FILES - {"usgs-2023-09.geojson"}:
        raise RuntimeError("QA03_BRONZE_CAPTURE_SET_CHANGED")
    for name, pin in source.items():
        if pin.get("sha256") != pins[name] or pin.get("source_system") != "JMA_BULLETIN" or pin.get("catalog_release") != "release-v1":
            raise RuntimeError("QA03_BRONZE_CAPTURE_CHANGED")
    usgs = report["usgs_bronze_input"]
    if usgs.get("sha256") != pins["usgs-2023-09.geojson"] or usgs.get("source_system") != "USGS":
        raise RuntimeError("QA03_USGS_CAPTURE_CHANGED")
    expected_inputs = [[source["h2000.zip"]], [source["h199701.zip"], source["h199710.zip"]],
                       [source["h199710.zip"]], [source["h2000.zip"]], [source["h2023.zip"], usgs]]
    for record, expected in zip(records[:5], expected_inputs):
        if record["context"]["resolver_request"]["bronze_inputs"] != expected:
            raise RuntimeError("QA03_CONTEXT_CAPTURE_CHANGED")
    evidence = report.get("case_evidence", {})
    if set(evidence) != set(qa03.REQUIRED_CASES):
        raise RuntimeError("QA03_CASE_EVIDENCE_MISSING")
    baseline, resumed, _, _, parent, changed = records
    snapshots = lambda record: {name: pin["snapshot_id"] for name, pin in record["commit"]["tables"].items()}
    stop = evidence["resume_after_failure"]
    if (stop.get("reason") != "QA_STOP_AFTER_DURABLE_SILVER" or stop.get("exception_class") != "java.io.IOException"
            or stop.get("process_restart") is not False or stop.get("downstream_blocked") is not True
            or stop.get("context") != resumed["context"] or stop.get("completed_receipts") != resumed["receipts"][:3]
            or stop.get("gold_baselines") != snapshots(baseline)
            or qa03._load(root / "resume-stop.json", qa03.MAX_EVIDENCE_BYTES, "STOP") != stop):
        raise RuntimeError("QA03_RESUME_EVIDENCE_INVALID")
    reuse = evidence["reuse_verified_archive"]
    if (reuse.get("operation_id") != resumed["operation_id"] or reuse.get("reused_receipt") != resumed["receipts"][1]
            or type(reuse.get("download_count")) is not int or reuse["download_count"] != 0):
        raise RuntimeError("QA03_REUSE_EVIDENCE_INVALID")
    release = evidence["changed_release"]
    new_pin = release.get("new_pin", {})
    if (release.get("before_operation_id") != parent["operation_id"] or release.get("after_operation_id") != changed["operation_id"]
            or release.get("old_pin") != source["h199701.zip"] or release.get("synthetic") is not True
            or release.get("old_snapshot_readback") is not True or new_pin.get("sha256") == pins["h199701.zip"]
            or new_pin.get("catalog_release") != "release-v2" or new_pin.get("year") != 1997 or new_pin.get("segment") != "jan-sep"
            or changed["context"]["resolver_request"]["bronze_inputs"] != [new_pin]
            or snapshots(parent)["event_current"] == snapshots(changed)["event_current"]):
        raise RuntimeError("QA03_RELEASE_EVIDENCE_INVALID")
    collision = evidence["same_release_different_bytes"]
    conflict = collision.get("context", {})
    etl.validate_context(conflict)
    conflict_pins = conflict["resolver_request"]["bronze_inputs"]
    if (collision.get("reason") != "JMA_RELEASE_IDENTITY_OR_ORDER_CONFLICT" or collision.get("downstream_blocked") is not True
            or collision.get("baselines_before") != snapshots(changed) or collision.get("baselines_after") != snapshots(changed)
            or conflict["resolver_request"]["namespace"] != request["namespace"] or len(conflict_pins) != 1
            or conflict_pins[0].get("catalog_release") != new_pin["catalog_release"] or conflict_pins[0].get("sha256") != pins["h199701.zip"]):
        raise RuntimeError("QA03_COLLISION_EVIDENCE_INVALID")
    guard = evidence["resource_guard"]
    if (guard.get("reason") != "BOUNDED_EXACT_MONTHS_REQUIRED" or guard.get("recovery_operation_id") != changed["operation_id"]
            or guard.get("baselines_before") != snapshots(changed) or guard.get("baselines_after") != snapshots(changed)
            or len(guard.get("resolver_request", {}).get("target_months", [])) != 25
            or guard["resolver_request"].get("namespace") != request["namespace"]):
        raise RuntimeError("QA03_RESOURCE_EVIDENCE_INVALID")
    retention = evidence["retention_outside_scope"]
    if (retention.get("month") != "2000-02" or retention.get("before_operation_id") != baseline["operation_id"]
            or retention.get("after_operation_id") != changed["operation_id"] or retention.get("old_snapshot_readback") is not True
            or retention.get("baseline_count") != baseline["commit"]["tables"]["event_current"]["count"] or retention["baseline_count"] <= 0
            or not qa03.SAFE_SHA256.fullmatch(str(retention.get("before_sha256", "")))
            or retention.get("before_sha256") != retention.get("after_sha256")):
        raise RuntimeError("QA03_RETENTION_EVIDENCE_INVALID")


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
    elif task == "QA-03":
        validate_history(request, report)
    elif task == "QA-04":
        qa04.validate(request, report, validate_publication)
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
    matrix_name = {"QA-02": "qa_02_rerun_revision_matrix.json", "QA-03": "qa_03_jma_historical_matrix.json", "QA-04": "qa_04_failure_recovery_matrix.json"}[task]
    matrix = json.loads((Path(__file__).parent / "fixtures" / matrix_name).read_text())
    matrix.setdefault("input_pins", {})["namespace"] = namespace
    _atomic_json(root / "matrix.json", matrix)
    request = {"task_id": task, "request_version": {"QA-02": "qa-02-runtime-v1", "QA-03": "qa03-fixed-request-v1", "QA-04": "qa04-fixed-request-v1"}[task], "run_id": run_id,
               "operation_id": run_id, "namespace": namespace, "lease_owner": owner,
               "matrix_path": str(root / "matrix.json"),
               "matrix_sha256": hashlib.sha256((root / "matrix.json").read_bytes()).hexdigest(),
               "report_path": str(root / "runtime-report.json")}
    if task == "QA-03":
        _, request["capture_pins_sha256"] = history_pins()
        request["inventory_sha256"] = qa03.load_matrix(root / "matrix.json", INVENTORY)["inventory_sha256"]
    _atomic_json(root / "request.json", request)
    print("event=pipeline_qa_started task=" + task + " evidence_root=" + str(root), flush=True)
    if task == "QA-02":
        qa02._validate_runtime_request(request)
    else:
        qa03._safe_path(request["report_path"], "QA03_OUTPUT_ALREADY_EXISTS", must_exist=False)
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
            # Retain ownership even if writing execution/capacity evidence
            # fails after a nonzero JVM exit with possible remote writes.
            unknown = process.returncode != 0
        capacity = {"duration_seconds": time.monotonic() - started,
                    "child_peak_rss_mb": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024,
                    "profile": "local[1], driver heap 768m, container limit 2GiB"}
        _atomic_json(root / "execution.json", {"job_class": JOBS[task], "exit_code": process.returncode,
                                               "request_sha256": hashlib.sha256((root / "request.json").read_bytes()).hexdigest(),
                                               "capacity": capacity})
        if process.returncode != 0:
            # A JVM exit alone cannot prove a remote Trino write has quiesced.
            unknown = True
            _atomic_json(root / "failure.json", {"status": "FAILED_LEASE_RETAINED", "exit_code": process.returncode})
            raise RuntimeError("PIPELINE_QA_FAILED_CHECK_SCOPED_PRIVATE_LOG")
        if task == "QA-04":
            try:
                report = qa04.finish_java(root, owner, env)
                _atomic_json(root / "runtime-report.json", report)
            except qa04.ProcessOutcomeUnknown:
                unknown = True
                raise
        else:
            report = json.loads((root / "runtime-report.json").read_text())
        try:
            validate(task, request, report)
        except Exception:
            _atomic_json(root / "failure.json", {"status": "REPORT_VALIDATION_FAILED", "remote_queries_returned": True})
            raise
        result = {"task": task, "status": "PASS", "namespace": namespace, "run_id": run_id,
                  "report_path": str(root / "runtime-report.json"),
                  "report_sha256": hashlib.sha256((root / "runtime-report.json").read_bytes()).hexdigest(),
                  "summary": report["summary"],
                  "capacity": capacity}
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
