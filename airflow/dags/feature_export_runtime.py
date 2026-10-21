"""MLD-05 process/lease/receipt control only. Java owns every record and Parquet operation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

from jma_backfill_runtime import _atomic_json
from source_run_guard import lease
from resource_pilot import java_identity
from pipeline_qa_acceptance import validate_publication

STAGING = Path("/opt/pipeline/staging/mld-05")


def validate_qa(report, request, root):
    if (report.get("task") != "MLD-05" or report.get("status") != "PASS"
            or report.get("namespace") != request["namespace"] or report.get("run_id") != request["run_id"]
            or report.get("synthetic_component_qa") is not True or report.get("research_ready") is not False
            or report.get("scientific_approval") is not False or report.get("required_month_count") != 225
            or report.get("partial_coverage_rejected") is not True or report.get("trusted_resolver_verified") is not True
            or len(report.get("publications", [])) != 10 or report.get("gold_before_export") != report.get("gold_after_export")):
        raise RuntimeError("MLD05_QA_REPORT_INVALID")
    months = []
    for publication in report["publications"]:
        validate_publication(publication, request["namespace"], root)
        months.extend(publication["context"]["resolver_request"]["target_months"])
    if len(months) != 225 or len(set(months)) != 225 or sorted(months)[0] != "2000-01" or sorted(months)[-1] != "2018-09":
        raise RuntimeError("MLD05_QA_HISTORY_INCOMPLETE")
    first, rerun = report["first"], report["rerun"]
    if first.get("reused") is not False:
        checkpoint, execution = root / "history-complete.json", root / "execution.json"
        if (first.get("reused") is not True or report.get("resumed_from_checkpoint") is not True
                or checkpoint.is_symlink() or not checkpoint.is_file() or execution.is_symlink() or not execution.is_file()
                or json.loads(execution.read_text()).get("exit_code") not in range(1, 256)):
            raise RuntimeError("MLD05_QA_UNPROVEN_EXPORT_RESUME")
    if (first.get("status") != "DATASET_EXPORTED" or rerun.get("status") != "DATASET_EXPORTED"
            or rerun.get("reused") is not True
            or first.get("feature_row_count", 0) <= 0 or first.get("mainshock_count", 0) <= 0
            or report["partial_dataset"]["dataset_id"] == first.get("dataset_id")):
        raise RuntimeError("MLD05_QA_EXPORT_INVALID")
    for name in ("registry_snapshot_id", "candidate_snapshot_id", "mainshock_snapshot_id"):
        if not re.fullmatch(r"[1-9][0-9]{0,18}", str(first.get(name, ""))) or int(first[name]) >= 2 ** 63 or first[name] != rerun.get(name):
            raise RuntimeError("MLD05_QA_PIN_CHANGED")
    for name in ("dataset_manifest_sha256", "bundle_manifest_sha256", "checksums_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}", str(first.get(name, ""))) or first[name] != rerun.get(name):
            raise RuntimeError("MLD05_QA_BUNDLE_CHANGED")


def run(qa=False, dataset_id=None, namespace=None, resume=None):
    if not qa and (not re.fullmatch(r"ds_[a-f0-9]{64}", dataset_id or "")
                   or not re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", namespace or "")):
        raise ValueError("MLD05_EXACT_DATASET_REQUIRED")
    if resume is not None and (not qa or not re.fullmatch(r"mld05-qa-[a-f0-9]{32}", resume)):
        raise ValueError("MLD05_EXACT_QA_RESUME_REQUIRED")
    run_id = resume or ("mld05-qa-" if qa else "mld05-export-") + uuid.uuid4().hex
    owner = {"dag_id": "mld05_feature_export", "run_id": run_id}
    root = STAGING / run_id
    if resume:
        if root.is_symlink() or not (root / "history-complete.json").is_file() or (root / "acceptance.json").exists():
            raise ValueError("MLD05_RESUME_CHECKPOINT_REQUIRED")
        request = json.loads((root / "request.json").read_text())
        if request.get("run_id") != run_id or request.get("lease_owner") != owner or lease("assert", owner) != {"status": "HELD", **owner}:
            raise RuntimeError("MLD05_RESUME_OWNER_CONFLICT")
    else:
        request = {"run_id": run_id, "dag_id": owner["dag_id"], "lease_owner": owner}
        if qa:
            request["namespace"] = os.environ["ICEBERG_CATALOG_NAME"] + ".mld05_qa_" + uuid.uuid4().hex
        else:
            request.update(ml_namespace=namespace, dataset_id=dataset_id)
        _atomic_json(root / "request.json", request)
    print("event=mld05_started evidence_root=" + str(root), flush=True)
    if not resume and lease("acquire", owner) != {"status": "ACQUIRED", **owner}:
        raise RuntimeError("MLD05_LEASE_UNCONFIRMED")
    unknown = False
    try:
        suffix = "-retry-" + uuid.uuid4().hex if resume else ""
        java_dir = root / ("java-identity" + suffix)
        java_dir.mkdir()
        job = "FeatureExportAcceptanceJob" if qa else "FeatureExportJob"
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m",
                   "--conf", "spark.driver.host=127.0.0.1", "--conf", "spark.driver.bindAddress=127.0.0.1",
                   "--conf", "spark.ui.enabled=false", "--jars",
                   "/opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar",
                   "--class", "ie212.earthquake.spark.ml." + job, "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(root / "request.json")]
        started = time.monotonic()
        with (root / ("stdout" + suffix + ".private.log")).open("xb") as out, (root / ("stderr" + suffix + ".private.log")).open("xb") as err:
            os.chmod(out.name, 0o600)
            os.chmod(err.name, 0o600)
            unknown = True
            process = subprocess.run(command, env=java_identity(dict(os.environ), java_dir), stdout=out, stderr=err, timeout=3600, check=False)
        unknown = process.returncode != 0
        _atomic_json(root / ("execution" + suffix + ".json"), {"exit_code": process.returncode, "job_class": job, "duration_seconds": time.monotonic() - started})
        if unknown:
            raise RuntimeError("MLD05_FAILED_LEASE_RETAINED_CHECK_SCOPED_LOG")
        report_file = root / ("runtime-report.json" if qa else "export-report.json")
        if report_file.is_symlink() or not report_file.is_file() or report_file.stat().st_size > 4 * 1024 * 1024:
            raise RuntimeError("MLD05_REPORT_REQUIRED")
        report = json.loads(report_file.read_text())
        if qa:
            validate_qa(report, request, root)
        elif report.get("status") != "DATASET_EXPORTED" or report.get("dataset_id") != dataset_id:
            raise RuntimeError("MLD05_EXPORT_NOT_READY")
        result = {"task": "MLD-05", "status": "PASS" if qa else "DATASET_EXPORTED", "run_id": run_id,
                  "report_sha256": hashlib.sha256(report_file.read_bytes()).hexdigest(), "report_path": str(report_file)}
    finally:
        # Nonzero/timeout is not proof that remote statements stopped. Never auto-takeover.
        if not unknown:
            if lease("assert", owner) != {"status": "HELD", **owner}:
                raise RuntimeError("MLD05_LEASE_CHANGED")
            released = lease("release", owner)
            if released != {"status": "RELEASED", **owner}:
                raise RuntimeError("MLD05_RELEASE_UNCONFIRMED")
            _atomic_json(root / "lease-release.json", released)
            if "result" in locals():
                result["lease_release"] = released
                _atomic_json(root / "acceptance.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--qa", action="store_true")
    parser.add_argument("--dataset-id")
    parser.add_argument("--namespace")
    parser.add_argument("--resume-qa")
    args = parser.parse_args()
    try:
        print(json.dumps(run(args.qa or bool(args.resume_qa), args.dataset_id, args.namespace, args.resume_qa), sort_keys=True))
    except Exception:
        raise SystemExit("MLD05_FAILED_CHECK_SCOPED_STAGING") from None
