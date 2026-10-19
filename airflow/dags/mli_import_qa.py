"""MLI-03 opt-in synthetic component acceptance on real Iceberg/MinIO/Trino.

Control plane only: Java seeds/validates/writes records. Each invocation allocates
fresh QA namespaces and keeps its evidence; no cleanup or pilot promotion.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import uuid

from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity
from source_run_guard import lease
from mli_03_import_results import run as run_import

STAGING = Path("/opt/pipeline/staging/mli-03-qa")
CHECKS = {"checksum", "schema", "duplicate", "unknown", "nan", "BUILDING", "DEVELOPMENT_PILOT",
          "renamed_pilot_marker", "trino_publication_pins",
          "rerun_unchanged", "changed_fingerprint", "partial_commit_hidden_until_candidate",
          "recovery_reuses_snapshot", "trino_blocker", "gold_unchanged"}


def validate_report(result, namespace):
    if (result.get("task") != "MLI-03" or result.get("status") != "PASSED"
            or result.get("namespace") != namespace or result.get("synthetic_component_qa") is not True
            or result.get("scientific_approval") is not False):
        raise RuntimeError("MLI03_QA_RECEIPT_INVALID")
    checks = result.get("checks", {})
    if not CHECKS.issubset(checks) or any(not checks[key] for key in CHECKS):
        raise RuntimeError("MLI03_QA_CHECKS_INCOMPLETE")
    publication = result.get("publication", {})
    if publication.get("experiment_status") != "CANDIDATE":
        raise RuntimeError("MLI03_QA_PUBLICATION_INVALID")
    for key in ("membership_snapshot_id", "summary_snapshot_id", "experiment_snapshot_id"):
        if type(publication.get(key)) is not int or publication[key] <= 0:
            raise RuntimeError("MLI03_QA_EXACT_PINS_REQUIRED")
    for key, expected in (("sequence_membership", 2), ("sequence_summary", 1), ("experiment_run", 1)):
        count = publication.get("readback", {}).get(key)
        if type(count) is not int or count != expected:
            raise RuntimeError("MLI03_QA_COUNTS_MISMATCH")


def run(environment=None):
    env = dict(os.environ if environment is None else environment)
    identity = {"dag_id": "mli_03_import_results", "run_id": "mli03-qa-" + uuid.uuid4().hex}
    namespace = env["ICEBERG_CATALOG_NAME"] + ".ml_qa_" + uuid.uuid4().hex
    root = STAGING / identity["run_id"]
    _atomic_json(root / "request.json", {"namespace": namespace, "lease_owner": identity})
    acquired = lease("acquire", identity, env)
    if acquired != {"status": "ACQUIRED", **identity}:
        raise RuntimeError("MLI03_QA_LEASE_UNCONFIRMED")
    unknown = False
    try:
        java_root = root / "java-identity"
        java_root.mkdir()
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "512m",
                   "--conf", "spark.sql.shuffle.partitions=1", "--conf", "spark.sql.adaptive.enabled=false",
                   "--conf", "spark.sql.codegen.wholeStage=false", "--conf", "spark.ui.enabled=false",
                   "--jars", "/opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar",
                   "--class", "ie212.earthquake.spark.ml.MlResultImportAcceptanceJob",
                   "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(root / "request.json")]
        with (root / "stdout.private.log").open("wb") as output, (root / "stderr.private.log").open("wb") as error:
            os.chmod(root / "stdout.private.log", 0o600)
            os.chmod(root / "stderr.private.log", 0o600)
            try:
                unknown = True
                process = subprocess.run(command, env=java_identity(env, java_root), stdout=output, stderr=error,
                                         timeout=1200, check=False)
                unknown = False
            except subprocess.TimeoutExpired:
                _atomic_json(root / "failure.json", {"status": "TIMEOUT_LEASE_RETAINED", "lease_owner": identity})
                raise RuntimeError("MLI03_QA_TIMEOUT_CHECK_PROCESS_BEFORE_RELEASE") from None
        if process.returncode != 0:
            _atomic_json(root / "failure.json", {"status": "FAILED", "exit_code": process.returncode})
            raise RuntimeError("MLI03_QA_FAILED_CHECK_PRIVATE_LOG")
        storage_report = root / "acceptance-report.json"
        report = json.loads(storage_report.read_text())
        # Java's storage receipt is not yet the full Python/lease acceptance.
        storage_report.rename(root / "storage-report.json")
        validate_report(report, namespace)
        # Exercise the production Python -> shell -> Java -> Trino boundary as
        # a same-bundle retry, not a fake runner returning a canned receipt.
        import_request = {"dag_id": identity["dag_id"], "run_id": identity["run_id"],
                          "dataset_id": "ds_fixture", "experiment_run_id": "exp_fixture",
                          "algorithm_name": "WINDOW", "model_config_version": "1",
                          "dataset_manifest_ref": "dataset.json",
                          "dataset_manifest_sha256": hashlib.sha256((root / "registry/dataset.json").read_bytes()).hexdigest(),
                          "artifact_root": str(root / "valid"), "journal_root": str(root / "journal"),
                          "ml_namespace": namespace, "import_run_id": identity["run_id"]}
        _atomic_json(root / "import-request.json", import_request)
        import_env = {**env, "MLI_DATASET_REGISTRY_ROOT": str(root / "registry"),
                      "MLI_IMPORT_RUNNER_COMMAND": "/opt/pipeline/bin/mli-import-runner"}
        # A timeout in the production subprocess also leaves the lease held.
        unknown = True
        control = run_import(root / "import-request.json", import_env, manage_lease=False)
        unknown = False
        if control.get("status") != "CANDIDATE" or control.get("readback") != report["publication"]["readback"]:
            raise RuntimeError("MLI03_QA_CONTROL_PLANE_MISMATCH")
        report["control_plane"] = control
        report["report_path"] = str(root / "acceptance-report.json")
        return report
    except Exception:
        if unknown and not (root / "failure.json").exists():
            _atomic_json(root / "failure.json", {"status": "OUTCOME_UNKNOWN_LEASE_RETAINED", "lease_owner": identity})
        raise
    finally:
        # local[1] has no remote executors; only a known exited process may release this owner.
        if not unknown:
            if lease("assert", identity, env) != {"status": "HELD", **identity}:
                raise RuntimeError("MLI03_QA_LEASE_OWNER_MISMATCH")
            if lease("release", identity, env) != {"status": "RELEASED", **identity}:
                raise RuntimeError("MLI03_QA_RELEASE_UNCONFIRMED")
            if "report" in locals() and "control_plane" in report:
                report["lease_release"] = {"status": "RELEASED", **identity}
                _atomic_json(root / "acceptance-report.json", report)


if __name__ == "__main__":
    try:
        print(json.dumps(run(), sort_keys=True))
    except Exception:
        raise SystemExit("MLI03_QA_FAILED_CHECK_SCOPED_STAGING") from None
