"""Opt-in GLD-04 control plane. All SQL/data validation and publishing remain Java."""
import json
import os
from pathlib import Path
import re
import subprocess
import uuid

from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity
from source_run_guard import lease

STAGING = Path("/opt/pipeline/staging/gold-publication-qa")


def run(reference, environment=None):
    env = dict(os.environ if environment is None else environment)
    reference = Path(reference)
    if reference.stat().st_size > 1_048_576:
        raise ValueError("BOUNDED_REFERENCE_REQUIRED")
    commit = json.loads(reference.read_text())["commit"]
    if not re.fullmatch(r"[a-z][a-z0-9_]*\.gold_qa_[a-f0-9]+", commit["namespace"]):
        raise ValueError("ISOLATED_QA_NAMESPACE_REQUIRED")
    run_id = "gld04-qa-" + uuid.uuid4().hex
    identity = {"dag_id": "gold_publication_qa", "run_id": run_id}
    root = STAGING / run_id
    lease("acquire", identity, env)
    completed = False
    try:
        request = {**identity, "gold_run_id": commit["gold_run_id"], "expected_namespace": commit["namespace"],
                   "expected_identity_sha256": commit["identity_sha256"]}
        _atomic_json(root / "request.json", request)
        identity_root = root / "java-identity"
        identity_root.mkdir()
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "512m",
            "--class", "ie212.earthquake.spark.GoldVerificationJob",
            "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(root / "request.json")]
        lease("assert", identity, env)
        with (root / "stdout.private.json").open("wb") as output, (root / "stderr.private.log").open("wb") as error:
            os.chmod(root / "stderr.private.log", 0o600)
            process = subprocess.run(command, env=java_identity(env, identity_root), stdout=output, stderr=error,
                                     timeout=1800, check=False)
        if process.returncode:
            raise RuntimeError("TRINO_VERIFICATION_FAILED_LEASE_RETAINED")
        report = json.loads((root / "gld-04-report.json").read_text())
        if report.get("published") is not True or report.get("rerun_unchanged") is not True \
                or report.get("engine") != "TRINO" or report.get("verify_status") != "PASSED" \
                or report.get("identity_sha256") != commit["identity_sha256"] or report.get("namespace") != commit["namespace"] \
                or report.get("gold_run_id") != commit["gold_run_id"] \
                or any(report.get("snapshot_bundle", {}).get(name, {}).get("snapshot_id") != pin["snapshot_id"]
                       for name, pin in commit["tables"].items()):
            raise RuntimeError("PUBLICATION_RECEIPT_INVALID")
        completed = True
        report["report_path"] = str(root / "gld-04-report.json")
        return report
    except BaseException:
        _atomic_json(root / "failure.json", {"task": "GLD-04", "status": "FAILED_LEASE_RETAINED",
            "reason": "check private log and exact Trino queries before recovery"})
        raise
    finally:
        # A nonzero/timeout JVM can leave a remote Trino statement running. No auto takeover.
        if completed:
            lease("release", identity, env)


if __name__ == "__main__":
    try:
        result = run("/opt/pipeline/gold-commit-reference.json")
        print(json.dumps({key: result[key] for key in ("task", "publication_id", "namespace", "event_count",
            "published", "verify_status", "rerun_unchanged", "report_path")}, sort_keys=True))
    except Exception:
        raise SystemExit("GLD04_QA_FAILED_CHECK_SCOPED_STAGING") from None
