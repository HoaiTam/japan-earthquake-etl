"""MLD-02 opt-in QA controller. Java owns Silver/Gold verification and Spark audit."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import uuid

from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity
from source_run_guard import lease

STAGING = Path("/opt/pipeline/staging/gold-input-audit-qa")
RUNNER = Path("/opt/spark/jobs/japan-earthquake-etl-runner.jar")


def validate_report(report, request, reference):
    publication = report.get("publication", {})
    pin = publication.get("snapshot_bundle", {}).get("event_current", {})
    if report.get("status") != "VERIFIED_BOUNDED_JMA_PILOT" or report.get("scope") != "BOUNDED_QA_ONLY" \
            or report.get("research_ready") is not False or report.get("rerun_unchanged") is not True \
            or report.get("gold_namespace") != request["gold_namespace"] or report.get("ml_namespace") != request["ml_namespace"] \
            or report.get("code_version") != request["code_version"] \
            or report.get("silver_bundle_sha256") != reference["attempts"][0]["bundle_manifest_sha256"] \
            or publication.get("published") is not True or publication.get("verify_status") != "PASSED" \
            or pin.get("table") != request["gold_namespace"] + ".event_current" \
            or not isinstance(pin.get("snapshot_id"), int) or pin["snapshot_id"] <= 0:
        raise RuntimeError("ML_QA_RECEIPT_INVALID")
    entries = report.get("datasets", [])
    if len(entries) != 2 or {entry.get("audit", {}).get("dataset_split") for entry in entries} != {"REPRODUCTION", "EXTENSION"}:
        raise RuntimeError("ML_QA_SPLITS_REQUIRED")
    for entry in entries:
        audit, manifest, handoff = entry["audit"], entry["manifest"], entry["audited_manifest"]
        counts = audit["counts"]
        mc = audit.get("mc_applied")
        if type(mc) not in (int, float) or not math.isfinite(mc) \
                or not re.fullmatch(r"ds_[a-f0-9]{64}", manifest.get("dataset_id", "")) \
                or audit.get("mc_method") != "MAXIMUM_CURVATURE" \
                or handoff.get("mc_value") != mc \
                or json.loads(audit["mc_config_json"]).get("applied_mc") != mc \
                or any(handoff.get(key) != value for key, value in counts.items()):
            raise RuntimeError("ML_QA_MC_HANDOFF_INVALID")
        if any(type(counts.get(key)) is not int or counts[key] < 0 for key in
               ("input_event_count", "eligible_event_count", "excluded_event_count")):
            raise RuntimeError("ML_QA_COUNTS_INVALID")
        if audit.get("input_mode") != "PUBLISHED_SNAPSHOT" or audit.get("candidate_build_allowed") is not True \
                or audit.get("completeness_overall", {}).get("is_reliable") is not True \
                or audit["completeness_overall"].get("sample_count", 0) < 50 \
                or entry.get("rerun_unchanged") is not True \
                or counts["input_event_count"] != pin.get("count") \
                or counts["input_event_count"] != counts["eligible_event_count"] + counts["excluded_event_count"] \
                or sum(audit["exclusion_counts_by_primary_reason"].values()) != counts["excluded_event_count"] \
                or entry.get("trino_input_count") != counts["input_event_count"] \
                or entry.get("trino_eligible_count") != counts["eligible_event_count"] \
                or hashlib.sha256(audit["mc_config_json"].encode()).hexdigest() != audit.get("mc_config_sha256"):
            raise RuntimeError("ML_QA_AUDIT_INVALID")
        for item in (audit, manifest, handoff):
            if item.get("gold_snapshot_id") != pin["snapshot_id"] or item.get("gold_publication_id") != publication["publication_id"] \
                    or item.get("dataset_id") != manifest.get("dataset_id"):
                raise RuntimeError("ML_QA_PIN_INVALID")
        if manifest.get("dataset_status") != "BUILDING" or handoff.get("dataset_status") != "BUILDING" \
                or handoff.get("mc_config_sha256") != audit["mc_config_sha256"]:
            raise RuntimeError("ML_QA_HANDOFF_INVALID")


def run(reference_path, environment=None):
    env = dict(os.environ if environment is None else environment)
    reference_path = Path(reference_path)
    if reference_path.stat().st_size > 1_048_576:
        raise ValueError("ML_BOUNDED_REFERENCE_REQUIRED")
    reference = json.loads(reference_path.read_text())
    if reference.get("status") != "VERIFIED_BOUNDED_SAMPLE" or not reference.get("attempts") \
            or not re.fullmatch(r"[a-z][a-z0-9_]*", env.get("ICEBERG_CATALOG_NAME", "")):
        raise ValueError("ML_VERIFIED_SILVER_REFERENCE_REQUIRED")
    suffix = uuid.uuid4().hex
    identity = {"dag_id": "gold_input_audit_qa", "run_id": "mld02-qa-" + suffix}
    # Hash the exact binary, not an unrelated main commit, for an uncommitted QA build.
    hasher = hashlib.sha256()
    with RUNNER.open("rb") as binary:
        for block in iter(lambda: binary.read(262_144), b""):
            hasher.update(block)
    digest = hasher.hexdigest()
    request = {**identity, "gold_namespace": env["ICEBERG_CATALOG_NAME"] + ".gold_qa_" + suffix,
               "ml_namespace": env["ICEBERG_CATALOG_NAME"] + ".ml_qa_" + suffix,
               "config_version": env.get("CONFIG_VERSION", "1"), "code_version": "artifact_" + digest}
    root = STAGING / identity["run_id"]
    lease("acquire", identity, env)
    completed = False
    try:
        _atomic_json(root / "request.json", request)
        identity_root = root / "java-identity"
        identity_root.mkdir()
        jars = "/opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar"
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "512m",
                   "--conf", "spark.sql.shuffle.partitions=1", "--conf", "spark.sql.adaptive.enabled=false",
                   "--conf", "spark.sql.codegen.wholeStage=false", "--conf", "spark.ui.enabled=false",
                   "--conf", "spark.driver.maxResultSize=64m", "--jars", jars,
                   "--class", "ie212.earthquake.spark.ml.GoldAuditAcceptanceJob", str(RUNNER),
                   str(root / "request.json"), str(reference_path)]
        lease("assert", identity, env)
        with (root / "stdout.private.log").open("wb") as output, (root / "stderr.private.log").open("wb") as error:
            os.chmod(root / "stdout.private.log", 0o600)
            os.chmod(root / "stderr.private.log", 0o600)
            process = subprocess.run(command, env=java_identity(env, identity_root), stdout=output, stderr=error,
                                     timeout=1800, check=False)
        if process.returncode:
            raise RuntimeError("ML_QA_JVM_FAILED_LEASE_RETAINED")
        report = json.loads((root / "mld-02-report.json").read_text())
        validate_report(report, request, reference)
        lease("assert", identity, env)
        completed = True
        return {"task": "MLD-02", "status": report["status"], "research_ready": False,
                "report_path": str(root / "mld-02-report.json"), **identity}
    finally:
        # No takeover/expiry: a failed JVM or remote Trino operation may still be active.
        if completed:
            lease("release", identity, env)


if __name__ == "__main__":
    try:
        print(json.dumps(run("/opt/pipeline/silver-reference.json"), sort_keys=True))
    except Exception:
        raise SystemExit("MLD02_QA_FAILED_CHECK_SCOPED_STAGING_AND_OWNER") from None
