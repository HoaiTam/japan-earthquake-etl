"""SLV-09 control plane only. Business parsing/linking/Gold transformation stays Java."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid
from datetime import datetime, timezone
from urllib.request import urlopen

from backfill_runtime import require
from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity, memory
from source_run_guard import lease

SUBMIT_TIMEOUT_SECONDS = 600


def idle():
    with urlopen("http://spark-master:8080/json/", timeout=10) as response:
        status = json.loads(response.read(65537))
    return not status.get("activeapps") and any(worker.get("state") == "ALIVE"
        and worker.get("coresfree", 0) >= 1 and worker.get("memoryfree", 0) >= 512
        for worker in status.get("workers", []))


def submit(request, root, environment):
    command = ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
        "--deploy-mode", "client", "--class", "ie212.earthquake.spark.SilverBronzeIntegrationJob",
        "--driver-memory", "512m", "--executor-memory", "512m", "--executor-cores", "1",
        "--conf", "spark.driver.host=spark-silver-integration", "--conf", "spark.driver.bindAddress=0.0.0.0",
        "--conf", "spark.cores.max=1", "--conf", "spark.dynamicAllocation.enabled=false",
        "--conf", "spark.sql.shuffle.partitions=1", "--conf", "spark.default.parallelism=1",
        "--conf", "spark.sql.adaptive.enabled=false", "--conf", "spark.sql.codegen.wholeStage=false",
        "--conf", "spark.driver.maxResultSize=64m", "--conf", "spark.ui.enabled=false",
        "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(request)]
    # Private logs remain in this run's scoped staging; never print payloads/credentials/errors.
    with tempfile.TemporaryDirectory(prefix="slv09-identity-") as identity_root, \
            (root / "stdout.tmp").open("w+b") as output, (root / "stderr.private.log").open("wb") as errors:
        os.chmod(root / "stderr.private.log", 0o600)
        process = subprocess.run(command, env=java_identity(environment, Path(identity_root)),
                                 stdout=output, stderr=errors, timeout=SUBMIT_TIMEOUT_SECONDS, check=False)
        if process.returncode != 0 or output.tell() > 65536:
            _atomic_json(root / "submit_failure.json", {"task": "SLV-09", "exit_code": process.returncode,
                         "stdout_bytes": output.tell(), "status": "FAILED"})
        require(process.returncode == 0 and output.tell() <= 65536, "SILVER_INTEGRATION_SUBMIT_FAILED")
        output.seek(0)
        return json.loads(output.read())


def confirmed_idle():
    # Allow asynchronous executor cleanup only after OUR submit finished. Never
    # steal/expire a lease, or retry source/publish operations automatically.
    for attempt in range(10):
        try:
            if idle():
                return True
        except Exception:
            return False
        if attempt < 9:
            time.sleep(0.5)
    return False


def run(environment=None):
    env = dict(os.environ if environment is None else environment)
    require(bool(env.get("CONFIG_VERSION")), "SILVER_CONFIG_REQUIRED")
    pins = json.loads((Path(__file__).parent / "fixtures/slv_09_bronze_inputs.json").read_text())["bronze_inputs"]
    scope = {"bronze_inputs": pins, "jma_records_per_archive": 256,
             "config_version": env["CONFIG_VERSION"], "integration_version": "slv09-live-v1",
             "window_start_utc": "1999-12-31T15:00:00Z", "window_end_utc": "2024-01-01T00:00:00Z",
             "is_backfill": True}
    scope_sha = hashlib.sha256(json.dumps(scope, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    run_id = "slv09-live-" + uuid.uuid4().hex
    identity = {"dag_id": "slv_09_integration_qa", "run_id": run_id}
    root = Path(env.get("BACKFILL_STAGING_ROOT", "/opt/pipeline/staging/backfill")) / "qa" / run_id
    request = root / "input.json"
    lease("acquire", identity, env)
    submitted = False
    try:
        require(idle(), "SILVER_CLUSTER_BUSY")
        before = memory()
        processed = datetime.now(timezone.utc)
        _atomic_json(request, {**scope, "run_id": run_id, "processing_date": processed.date().isoformat(),
            "processed_at_utc": processed.isoformat().replace("+00:00", "Z"),
            "contract_version": "orc-03-v1", "observability_version": "orc-04-v1", "scope_sha256": scope_sha})
        reports = []
        for attempt in range(2):
            lease("assert", identity, env)
            require(idle(), "SILVER_CLUSTER_BUSY")
            submitted = True
            report = submit(request, root, env)
            require(report.get("task") == "SLV-09" and report.get("run_id") == run_id
                and report.get("scope_sha256") == scope_sha
                and report.get("silver_status") == "SilverReady" and report.get("gold_handoff_verified") is True
                and report.get("gold_published") is False and report.get("master") == "spark://spark-master:7077"
                and str(report.get("java_version", "")).startswith("17.")
                and report.get("idempotent_reuse") is bool(attempt)
                and report.get("reconciliation", {}).get("balanced") is True
                and set(report.get("datasets", {})) == {"source_observation", "reject_record", "source_link", "canonical_membership"},
                "SILVER_RECEIPT_INVALID")
            _atomic_json(root / f"attempt-{attempt + 1}.json", report)
            reports.append(report)
            require(confirmed_idle(), "SILVER_LEASE_RETAINED_CHECK_ACTIVE_APPLICATION")
        for key in ("bundle_manifest_sha256", "identity_sha256", "reconciliation", "datasets",
                    "gold_canonical_rows", "gold_bridge_rows", "sources", "scope_sha256"):
            require(reports[0][key] == reports[1][key], "SILVER_RERUN_CHANGED")
        after = memory()
        require(before.get("events") is not None and after.get("events") is not None
                and before["events"]["oom_kill"] == after["events"]["oom_kill"], "SILVER_INTEGRATION_OOM")
        result = {"task": "SLV-09", "run_id": run_id, "scope_sha256": scope_sha,
            "rerun_unchanged": True, "attempts": reports, "driver_cgroup_before": before,
            "driver_cgroup_after": after, "report_path": str(root / "integration_report.json")}
    except Exception as error:
        # Only type/scope, never exception text/argv/env or source payload.
        _atomic_json(root / "failure.json", {"task": "SLV-09", "run_id": run_id,
                     "scope_sha256": scope_sha, "status": "FAILED", "error_type": type(error).__name__})
        raise
    finally:
        # Timeout/killed driver does not prove distributed executors are finished.
        # Keep lease fail-closed if the cluster cannot confirm termination/idle.
        if submitted:
            require(confirmed_idle(), "SILVER_LEASE_RETAINED_CHECK_ACTIVE_APPLICATION")
        require(lease("release", identity, env)["status"] == "RELEASED", "SILVER_LEASE_NOT_RELEASED")
    result["lease_released"] = True
    _atomic_json(root / "integration_report.json", result)
    return result


if __name__ == "__main__":
    try:
        print(json.dumps(run(), sort_keys=True))
    except Exception:
        raise SystemExit("SLV09_QA_FAILED_CHECK_SCOPED_STAGING_AND_SOURCE_LEASE") from None
