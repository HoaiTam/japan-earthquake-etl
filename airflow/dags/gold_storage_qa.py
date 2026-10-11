"""GLD-03 opt-in control plane: pinned capture, shared lease, bounded Java smoke."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid
from datetime import datetime, timezone

from source_run_guard import lease
from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity

CAPTURE = Path("/opt/pipeline/fixtures/usgs_2023_window.geojson")
STAGING = Path("/opt/pipeline/staging/gold-qa")

def run(environment=None):
    env = dict(os.environ if environment is None else environment)
    capture = CAPTURE
    run_id = "gld03-qa-" + uuid.uuid4().hex
    root = STAGING / run_id
    identity = {"dag_id": "gold_storage_qa", "run_id": run_id}
    lease("acquire", identity, env)
    outcome_unknown = False
    try:
        request = {"run_id": run_id, "namespace": env["ICEBERG_CATALOG_NAME"] + ".gold_qa_" + uuid.uuid4().hex,
            "processed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "config_version": env["CONFIG_VERSION"], "code_version": env["GOLD_CODE_VERSION"],
            "capture_sha256": hashlib.sha256(capture.read_bytes()).hexdigest()}
        _atomic_json(root / "request.json", request)
        identity_root = root / "java-identity"
        identity_root.mkdir()
        submit_env = java_identity(env, identity_root)
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "512m",
            "--conf", "spark.sql.shuffle.partitions=1", "--conf", "spark.sql.adaptive.enabled=false",
            "--conf", "spark.sql.codegen.wholeStage=false", "--conf", "spark.ui.enabled=false",
            "--jars", "/opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar",
            "--class", "ie212.earthquake.spark.usgs.GoldIcebergAcceptanceJob",
            "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(root / "request.json"), str(capture)]
        lease("assert", identity, env)
        with (root / "stdout.json").open("wb") as output, (root / "stderr.private.log").open("wb") as error:
            os.chmod(root / "stderr.private.log", 0o600)
            try:
                outcome_unknown = True
                process = subprocess.run(command, env=submit_env, stdout=output, stderr=error, timeout=1200, check=False)
                outcome_unknown = False
            except subprocess.TimeoutExpired:
                _atomic_json(root / "failure.json", {"task": "GLD-03", "status": "TIMEOUT_LEASE_RETAINED"})
                raise RuntimeError("GOLD_QA_TIMEOUT_CHECK_PROCESS_BEFORE_RELEASE") from None
        if process.returncode != 0:
            _atomic_json(root / "failure.json", {"task": "GLD-03", "exit_code": process.returncode})
            raise RuntimeError("GOLD_QA_FAILED_CHECK_PRIVATE_LOG")
        result = json.loads((root / "gld-03-report.json").read_text())
        if not result.get("rerun_unchanged") or result["commit"].get("published") is not False:
            raise RuntimeError("GOLD_QA_RECEIPT_INVALID")
        result["report_path"] = str(root / "gld-03-report.json")
        return result
    finally:
        # local[1]: completed JVM has no standalone executors. Timeout keeps owner fail-closed.
        if not outcome_unknown:
            lease("release", identity, env)


if __name__ == "__main__":
    try:
        report = run()
        print(json.dumps({key: report[key] for key in ("task", "java_version", "spark_version", "rerun_unchanged", "report_path")}
            | {"namespace": report["commit"]["namespace"], "published": report["commit"]["published"],
               "tables": {name: {key: pin[key] for key in ("snapshot_id", "count")}
                          for name, pin in report["commit"]["tables"].items()}}, sort_keys=True))
    except Exception:
        raise SystemExit("GLD03_QA_FAILED_CHECK_SCOPED_STAGING") from None
