"""QA-04 fixed storage evidence, separate from the external qa-04-report-v1 API.

Only ownership, process and JSON/receipt checks. Java owns all ETL/Parquet/Iceberg.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

from jma_backfill_runtime import _atomic_json
from source_run_guard import lease, SourceRunBusy
import real_etl_runtime as etl
import qa_03_jma_historical_backfill as paths

ETL_CASES = {"fetch_timeout", "archive_invalid", "silver_failure", "gold_commit_partial",
             "gold_commit_receipt_lost", "trino_verify_failure", "publication_metadata_failure"}
ML_TABLES = {"sequence_membership", "sequence_summary", "experiment_run"}


class ProcessOutcomeUnknown(RuntimeError):
    pass


def timeout_contender(owner, environment):
    """A disposable no-I/O child, NOT a real writer kill or a remote-query timeout."""
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    timed_out = False
    blocked = False
    contender = {"dag_id": "qa04_contender", "run_id": owner["run_id"] + "-contender"}
    foreign_release = None
    try:
        try:
            child.wait(timeout=0.05)
        except subprocess.TimeoutExpired:
            timed_out = child.poll() is None
        if not timed_out:
            raise RuntimeError("QA04_EXPECTED_LIVE_CHILD_TIMEOUT")
        try:
            lease("acquire", contender, environment)
        except SourceRunBusy:
            blocked = True
        foreign_release = lease("release", contender, environment)
        if not blocked or foreign_release["status"] != "NOT_OWNER" or lease("assert", owner, environment)["status"] != "HELD":
            raise RuntimeError("QA04_CONTENDER_DID_NOT_RESPECT_OWNER")
    finally:
        if child.poll() is None:
            child.terminate()  # Only the disposable process created above; never a JVM/service/query.
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            raise ProcessOutcomeUnknown("QA04_CONTROLLED_PROCESS_NOT_QUIESCENT") from None
    if lease("assert", owner, environment)["status"] != "HELD":
        raise RuntimeError("QA04_LEASE_LOST_AFTER_QUIESCENCE")
    return {"case_id": "lease_timeout_contender", "status": "PASS", "reason_code": "TIMEOUT_LEASE_RETAINED",
            "timeout_observed": True, "writer_alive_at_timeout": True, "controlled_child_has_no_io": True,
            "contender_blocked": blocked, "foreign_release": foreign_release["status"],
            "quiescent": child.poll() is not None, "child_exit_code": child.returncode,
            "owner_held_after_quiescence": owner, "release_deferred_to_final_acceptance": True}


def finish_java(root, owner, environment):
    report = paths._load(root / "java-report.json", 4 * 1024 * 1024, "JAVA_REPORT")
    if report.get("status") != "JAVA_PASSED_LEASE_PENDING" or len(report.get("cases", [])) != 9:
        raise RuntimeError("QA04_JAVA_CASES_INCOMPLETE")
    control = timeout_contender(owner, environment)
    _atomic_json(root / "lease-timeout-evidence.json", control)
    report["cases"].append(control)
    report["status"] = "PASS"
    report["summary"] = {"case_count": 10, "passed": 10, "failed": 0, "pending_runtime": 0}
    return report


def snapshots(record):
    return {name: pin["snapshot_id"] for name, pin in record["commit"]["tables"].items()}


def _exact_pins(value):
    return (isinstance(value, dict) and set(value) == ML_TABLES
            and all(type(pin) is int and 0 < pin < 2 ** 63 for pin in value.values()))


def ml_candidate(record, namespace, expected_id):
    if (record.get("namespace") != namespace or record.get("experiment_status") != "CANDIDATE"
            or record.get("experiment_run_id") != expected_id or record.get("dataset_id") != "ds_fixture"
            or record.get("readback") != {"sequence_membership": 2, "sequence_summary": 1, "experiment_run": 1}
            or not paths.SAFE_SHA256.fullmatch(str(record.get("result_bundle_sha256", "")))
            or not _exact_pins({name: record.get(key) for name, key in
                               (("sequence_membership", "membership_snapshot_id"),
                                ("sequence_summary", "summary_snapshot_id"), ("experiment_run", "experiment_snapshot_id"))})):
        raise RuntimeError("QA04_ML_PUBLICATION_INVALID")


def validate(request, report, validate_publication):
    root = paths._safe_path(request["report_path"], "QA04_REPORT_MISSING", must_exist=True).parent
    matrix = paths._load(Path(request["matrix_path"]), 256 * 1024, "MATRIX")
    matrix_sha = paths._sha256_file(Path(request["matrix_path"]), "QA04_MATRIX_UNREADABLE")
    expected = {case["case_id"]: case for case in matrix["cases"]}
    if (matrix.get("task_id") != "QA-04" or matrix.get("version") != "qa04-v1"
            or set(expected) != ETL_CASES | {"ml_bundle_invalid", "ml_import_partial", "lease_timeout_contender"}
            or matrix_sha != request["matrix_sha256"] or report.get("matrix_sha256") != matrix_sha
            or report.get("task") != "QA-04" or report.get("schema_version") != "qa04-storage-acceptance-v1"
            or report.get("namespace") != request["namespace"] or report.get("run_id") != request["run_id"]
            or report.get("synthetic_fixture") is not True or report.get("status") != "PASS"
            or report.get("summary") != {"case_count": 10, "passed": 10, "failed": 0, "pending_runtime": 0}
            or report.get("outside_month") != "2023-08"
            or not paths.SAFE_SHA256.fullmatch(str(report.get("outside_before_sha256", "")))
            or report["outside_before_sha256"] != report.get("outside_after_sha256")):
        raise RuntimeError("QA04_REPORT_IDENTITY_OR_RETENTION_INVALID")
    cases = report.get("cases", [])
    if len(cases) != 10 or {case.get("case_id") for case in cases} != set(expected):
        raise RuntimeError("QA04_CASES_INCOMPLETE")
    parent = report["baseline"]
    validate_publication(parent, request["namespace"], root)
    if parent["context"]["resolver_request"]["target_months"] != ["2023-08"] or parent["commit"]["tables"]["event_current"]["count"] <= 0:
        raise RuntimeError("QA04_POSITIVE_BASELINE_REQUIRED")
    for case in cases[:7]:
        id = case.get("case_id")
        if (id not in ETL_CASES or case.get("status") != "PASS" or case.get("downstream_blocked") is not True
                or case.get("outside_scope_unchanged") is not True or case.get("recovery_status") != "RECOVERED"
                or case.get("outside_before_sha256") != report["outside_before_sha256"]
                or case.get("outside_after_sha256") != report["outside_before_sha256"]):
            raise RuntimeError("QA04_ETL_CASE_INVALID")
        fault, recovered = case["fault"], case["recovery"]
        validate_publication(recovered, request["namespace"], root)
        if (fault.get("context") != recovered["context"] or fault.get("operation_id") != recovered["operation_id"]
                or fault.get("baseline_snapshots") != snapshots(parent)
                or recovered["context"]["resolver_request"]["target_months"] != expected[id]["scope"]
                or recovered["silver"]["affected_months"] != expected[id]["scope"]
                or fault.get("fault_stage") != expected[id]["fault_stage"] or fault.get("reason_code") != expected[id]["reason_code"]
                or fault.get("published_before_recovery") is not False or fault.get("downstream_blocked") is not True
                or fault.get("dataset_gate_reason") != "DS_GOLD_NOT_PUBLISHED"
                or type(fault.get("publication_rows_before_recovery")) is not int or fault["publication_rows_before_recovery"] != 0
                or paths._load(root / (id + "-fault.json"), 4 * 1024 * 1024, "FAULT") != fault):
            raise RuntimeError("QA04_FAULT_RECOVERY_BINDING_INVALID")
        partial = fault.get("partial_snapshots", {})
        if set(partial) != etl.TABLES:
            raise RuntimeError("QA04_PARTIAL_PINS_REQUIRED")
        changed = {name for name in partial if partial[name] != snapshots(parent)[name]}
        if id == "gold_commit_partial" and not 0 < len(changed) < 6:
            raise RuntimeError("QA04_PARTIAL_COMMIT_NOT_PROVEN")
        if id == "gold_commit_receipt_lost" and (len(changed) != 6 or fault.get("commit_receipt_present_at_failure") is not False):
            raise RuntimeError("QA04_LOST_RECEIPT_NOT_PROVEN")
        if id in {"trino_verify_failure", "publication_metadata_failure"} and len(changed) != 6:
            raise RuntimeError("QA04_COMMIT_BEFORE_VERIFICATION_REQUIRED")
        if id in {"fetch_timeout", "archive_invalid", "silver_failure"} and changed:
            raise RuntimeError("QA04_UPSTREAM_FAILURE_CHANGED_GOLD")
        if any(partial[name] != snapshots(recovered)[name] for name in changed):
            raise RuntimeError("QA04_RECOVERY_RECOMMITTED_TABLE")
        if id == "fetch_timeout":
            fetch = fault.get("fetch", {})
            if (fetch.get("reason") != "HttpTimeoutException" or fetch.get("same_request") is not True
                    or fetch.get("request_count") != 2 or fetch.get("recovery_http_status") != 200):
                raise RuntimeError("QA04_FETCH_RECOVERY_NOT_PROVEN")
        if id == "archive_invalid" and (fault.get("rejected_attempt") != 1 or fault.get("verified_attempt") != 2 or fault.get("rejected_before_ready") is not True):
            raise RuntimeError("QA04_ARCHIVE_ATTEMPT_INVALID")
        parent = recovered
    ml = report["ml"]
    namespace = request["namespace"].replace(".qa04_", ".qa04_ml_")
    if (ml.get("namespace") != namespace or ml.get("synthetic_component_qa") is not True or ml.get("scientific_approval") is not False
            or ml.get("outside_experiment_run_id") != "exp_outside" or ml.get("old_snapshots_readable") is not True
            or not paths.SAFE_SHA256.fullmatch(str(ml.get("outside_before_sha256", "")))
            or ml.get("outside_before_sha256") != ml.get("outside_after_sha256")
            or report.get("gold_before_ml") != snapshots(parent) or report.get("gold_after_ml") != snapshots(parent)
            or paths._load(root / "ml/ml-evidence.json", 4 * 1024 * 1024, "ML") != ml):
        raise RuntimeError("QA04_ML_RETENTION_OR_GOLD_CHANGED")
    ml_candidate(ml["baseline"], namespace, "exp_outside")
    invalid = ml["ml_bundle_invalid"]
    if len(invalid.get("rejections", [])) != 2:
        raise RuntimeError("QA04_ML_REJECTIONS_MISSING")
    for rejection, kind in zip(invalid["rejections"], ("checksum", "schema")):
        if (rejection.get("experiment_run_id") != "exp_bad_" + kind
                or rejection.get("reason") != {"checksum": "IMP_CHECKSUM_MISMATCH", "schema": "IMP_SCHEMA_MISMATCH"}[kind]
                or rejection.get("candidate_rows") != 0 or rejection.get("journal", {}).get("experiment_status") != "REJECTED"
                or not _exact_pins(rejection.get("snapshots_before")) or rejection["snapshots_before"] != rejection.get("snapshots_after")):
            raise RuntimeError("QA04_ML_INVALID_BUNDLE_PUBLISHED")
    ml_candidate(invalid["recovery"], namespace, "exp_recovered")
    partial = ml["ml_import_partial"]
    ml_candidate(partial["recovery"], namespace, "exp_partial")
    if (partial.get("fault_journal", {}).get("experiment_status") != "IMPORT_VALIDATING"
            or partial.get("candidate_journal") != partial["recovery"] or partial.get("candidate_rows_before_recovery") != 0
            or partial.get("membership_snapshot_before_recovery") != partial["recovery"]["membership_snapshot_id"]):
        raise RuntimeError("QA04_ML_PARTIAL_RECOVERY_INVALID")
    for case in cases[7:9]:
        if case.get("status") != "PASS" or case.get("evidence") != ml.get(case.get("case_id")):
            raise RuntimeError("QA04_ML_CASE_BINDING_INVALID")
    control = cases[-1]
    if (control.get("case_id") != "lease_timeout_contender" or control.get("status") != "PASS"
            or any(control.get(key) is not True for key in ("timeout_observed", "writer_alive_at_timeout", "controlled_child_has_no_io",
                                                           "contender_blocked", "quiescent", "release_deferred_to_final_acceptance"))
            or control.get("foreign_release") != "NOT_OWNER" or control.get("owner_held_after_quiescence") != request["lease_owner"]
            or type(control.get("child_exit_code")) is not int
            or paths._load(root / "lease-timeout-evidence.json", 65536, "LEASE") != control):
        raise RuntimeError("QA04_LEASE_RECOVERY_INVALID")
