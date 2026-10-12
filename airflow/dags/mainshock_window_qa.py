"""Opt-in MLD-03 acceptance controller; all catalog selection/transforms stay in Java."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity
from source_run_guard import lease

STAGING = Path("/opt/pipeline/staging/mainshock-window-qa")
RUNNER = Path("/opt/spark/jobs/japan-earthquake-etl-runner.jar")


def digest(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(262144), b""):
            hasher.update(block)
    return hasher.hexdigest()


def validate_report(report, request, root):
    pub = report.get("publication", {})
    pin = pub.get("snapshot_bundle", {}).get("event_current", {})
    window_code = request.get("window_code_version", request["code_version"])
    if report.get("status") != "VERIFIED_PUBLISHED_JMA_WINDOWS" or report.get("scope") != "BOUNDED_QA_ONLY" \
            or report.get("research_ready") is not False or report.get("rerun_unchanged") is not True \
            or report.get("trino_microsecond_gate_passed") is not True \
            or pub.get("verify_status") != "PASSED" or pub.get("published") is not True \
            or pin.get("table") != request["gold_namespace"] + ".event_current" \
            or report.get("window_code_version", report.get("code_version")) != window_code \
            or any(report.get(key) != request[key] for key in ("gold_namespace", "ml_namespace", "code_version", "run_id")):
        raise RuntimeError("ML_WINDOW_QA_REPORT_INVALID")
    datasets = report.get("datasets", [])
    if len(datasets) != 2 or {item.get("manifest", {}).get("dataset_split") for item in datasets} != {"REPRODUCTION", "EXTENSION"}:
        raise RuntimeError("ML_WINDOW_QA_SPLITS_REQUIRED")
    for item in datasets:
        manifest, audit, window, handoff = (item[key] for key in ("manifest", "audit", "window", "window_manifest"))
        split = manifest["dataset_split"].lower()
        if item.get("rerun_unchanged") is not True or not re.fullmatch(r"ds_[a-f0-9]{64}", manifest.get("dataset_id", "")) \
                or manifest.get("code_version") != window_code \
                or manifest.get("dataset_status") != "BUILDING" or handoff.get("dataset_status") != "BUILDING" \
                or window.get("research_ready") is not False or window.get("parquet_readback_verified") is not True \
                or window.get("bucket_join_verified") is not True or window.get("mainshock_count", 0) < 1 \
                or window.get("rejected_window_count") != 0 or window.get("flagged_window_count") != 0:
            raise RuntimeError("ML_WINDOW_QA_NONEMPTY_READBACK_REQUIRED")
        for entry in (audit, window, handoff):
            if entry.get("dataset_id") != manifest["dataset_id"] or entry.get("gold_snapshot_id") != pin.get("snapshot_id") \
                    or entry.get("gold_publication_id") != pub.get("publication_id"):
                raise RuntimeError("ML_WINDOW_QA_PIN_INVALID")
        counts = audit["counts"]
        if audit.get("input_mode") != "PUBLISHED_SNAPSHOT" or audit.get("candidate_build_allowed") is not True \
                or counts["input_event_count"] != pin.get("count") \
                or counts["input_event_count"] != counts["eligible_event_count"] + counts["excluded_event_count"] \
                or sum(audit["exclusion_counts_by_primary_reason"].values()) != counts["excluded_event_count"] \
                or any(handoff.get(key) != value for key, value in counts.items()):
            raise RuntimeError("ML_WINDOW_QA_AUDIT_COUNTS_INVALID")
        mc = audit.get("mc_applied")
        if type(mc) not in (int, float) or not math.isfinite(mc) or window.get("mc_applied") != mc or handoff.get("mc_value") != mc \
                or hashlib.sha256(audit["mc_config_json"].encode()).hexdigest() != audit.get("mc_config_sha256") \
                or any(entry.get("mc_config_sha256") != audit["mc_config_sha256"] for entry in (window, handoff)):
            raise RuntimeError("ML_WINDOW_QA_MC_INVALID")
        roles = window["role_counts"]
        oracle = item["trino_oracle"]
        if len(oracle) != window["mainshock_count"] or roles.get("MAINSHOCK") != window["mainshock_count"] \
                or roles.get("PRE", 0) < 1 or roles.get("POST", 0) < 1 \
                or sum(roles.values()) != window["total_candidate_row_count"] \
                or sum(row["candidate_count"] for row in oracle) != window["total_candidate_row_count"] \
                or sum(row["pre_count"] for row in oracle) != roles["PRE"] \
                or sum(row["post_count"] for row in oracle) != roles["POST"] \
                or any(row["self_count"] != 1 or row["candidate_count"] != window["windows"][row["mainshock_event_id"]]["candidate_count"] for row in oracle) \
                or handoff.get("mainshock_count") != window["mainshock_count"] \
                or handoff.get("candidate_row_count") != window["total_candidate_row_count"]:
            raise RuntimeError("ML_WINDOW_QA_ORACLE_INVALID")
        files = window.get("parquet_files", [])
        if not files or len(files) > 1024:
            raise RuntimeError("ML_WINDOW_QA_ARTIFACTS_REQUIRED")
        base = (root / split).resolve()
        for file in files:
            path = (base / file["path"]).resolve()
            if not path.is_relative_to(base) or not path.is_file() or path.stat().st_size != file["bytes"] or digest(path) != file["sha256"]:
                raise RuntimeError("ML_WINDOW_QA_ARTIFACT_CHANGED")


def run(pins_path, environment=None):
    env = dict(os.environ if environment is None else environment)
    pins_path = Path(pins_path)
    if pins_path.stat().st_size > 65536 or not re.fullmatch(r"[a-z][a-z0-9_]*", env.get("ICEBERG_CATALOG_NAME", "")):
        raise ValueError("ML_WINDOW_QA_BOUNDED_INPUT_REQUIRED")
    pins = json.loads(pins_path.read_text())
    if len(pins.get("bronze_inputs", [])) != 3:
        raise ValueError("ML_WINDOW_QA_EXACT_PINS_REQUIRED")
    suffix = uuid.uuid4().hex
    identity = {"dag_id": "mainshock_window_qa", "run_id": "mld03-qa-" + suffix}
    request = {**identity, "gold_namespace": env["ICEBERG_CATALOG_NAME"] + ".gold_qa_" + suffix,
               "ml_namespace": env["ICEBERG_CATALOG_NAME"] + ".ml_qa_" + suffix,
               "code_version": "artifact_" + digest(RUNNER), "config_version": env.get("CONFIG_VERSION", "1")}
    root = STAGING / identity["run_id"]
    lease("acquire", identity, env)
    return _execute(request, root, pins_path, env)


def resume(run_id, pins_path, environment=None):
    """Continue only our retained QA lease; never acquire/take over another or reseed Gold."""
    env = dict(os.environ if environment is None else environment)
    if not re.fullmatch(r"mld03-qa-[a-f0-9]{32}", run_id):
        raise ValueError("ML_WINDOW_QA_EXACT_RESUME_RUN_REQUIRED")
    original = STAGING / run_id / "request.json"
    if original.stat().st_size > 65536 or Path(pins_path).stat().st_size > 65536:
        raise ValueError("ML_WINDOW_QA_BOUNDED_INPUT_REQUIRED")
    request = json.loads(original.read_text())
    if request.get("run_id") != run_id or request.get("dag_id") != "mainshock_window_qa":
        raise ValueError("ML_WINDOW_QA_RESUME_IDENTITY_INVALID")
    identity = {key: request[key] for key in ("dag_id", "run_id")}
    lease("assert", identity, env)
    request["window_code_version"] = "artifact_" + digest(RUNNER)
    root = STAGING / run_id / ("resume-" + uuid.uuid4().hex)
    return _execute(request, root, Path(pins_path), env, resume_mode=True)


def _execute(request, root, pins_path, env, resume_mode=False):
    identity = {key: request[key] for key in ("dag_id", "run_id")}
    completed = False
    try:
        _atomic_json(root / "request.json", request)
        identity_root = root / "java-identity"
        identity_root.mkdir()
        jars = "/opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar"
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m", "--jars", jars,
                   "--conf", "spark.sql.shuffle.partitions=2", "--conf", "spark.sql.adaptive.enabled=false",
                   "--conf", "spark.sql.codegen.wholeStage=false", "--conf", "spark.ui.enabled=false",
                   "--conf", "spark.driver.maxResultSize=64m", "--conf", "spark.sql.autoBroadcastJoinThreshold=-1",
                   "--class", "ie212.earthquake.spark.ml.MainshockWindowAcceptanceJob", str(RUNNER),
                   str(root / "request.json"), str(pins_path)]
        if resume_mode:
            command.append("--resume-windows")
        lease("assert", identity, env)
        with (root / "stdout.private.log").open("wb") as output, (root / "stderr.private.log").open("wb") as error:
            os.chmod(root / "stdout.private.log", 0o600)
            os.chmod(root / "stderr.private.log", 0o600)
            process = subprocess.run(command, env=java_identity(env, identity_root), stdout=output, stderr=error, timeout=3600, check=False)
        if process.returncode:
            raise RuntimeError("ML_WINDOW_QA_FAILED_LEASE_RETAINED")
        path = root / "mld-03-report.json"
        if path.stat().st_size > 2_097_152:
            raise RuntimeError("ML_WINDOW_QA_REPORT_SIZE_LIMIT")
        validate_report(json.loads(path.read_text()), request, root)
        lease("assert", identity, env)
        completed = True
        return {**identity, "task": "MLD-03", "status": "VERIFIED_PUBLISHED_JMA_WINDOWS", "report_path": str(path), "research_ready": False}
    finally:
        if completed:
            lease("release", identity, env)


if __name__ == "__main__":
    try:
        if len(sys.argv) == 3 and sys.argv[1] == "--resume-windows":
            result = resume(sys.argv[2], "/opt/pipeline/window-bronze-pins.json")
        elif len(sys.argv) == 1:
            result = run("/opt/pipeline/window-bronze-pins.json")
        else:
            raise ValueError("ML_WINDOW_QA_ARGUMENTS_INVALID")
        print(json.dumps(result, sort_keys=True))
    except Exception:
        raise SystemExit("MLD03_QA_FAILED_CHECK_SCOPED_STAGING_AND_OWNER") from None
