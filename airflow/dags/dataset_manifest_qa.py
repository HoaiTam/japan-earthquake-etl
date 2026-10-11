"""MLD-01 opt-in metadata smoke. Java owns all identity and Iceberg SQL."""
import json
import os
from pathlib import Path
import re
import subprocess
import uuid

from jma_backfill_runtime import _atomic_json
from resource_pilot import java_identity
from source_run_guard import lease

STAGING = Path("/opt/pipeline/staging/dataset-manifest-qa")


def run(reference, config_path, environment=None):
    env = dict(os.environ if environment is None else environment)
    reference, config_path = Path(reference), Path(config_path)
    if reference.stat().st_size > 1_048_576 or config_path.stat().st_size > 65536:
        raise ValueError("DS_BOUNDED_INPUT_REQUIRED")
    publication = json.loads(reference.read_text())
    namespace = publication.get("namespace", "")
    if not re.fullmatch(r"[a-z][a-z0-9_]*\.gold_qa_[a-f0-9]+", namespace) \
            or publication.get("published") is not True or publication.get("verify_status") != "PASSED":
        raise ValueError("DS_PUBLISHED_QA_REFERENCE_REQUIRED")
    config = json.loads(config_path.read_text())
    identity = {"dag_id": "dataset_manifest_qa", "run_id": "mld01-qa-" + uuid.uuid4().hex}
    root = STAGING / identity["run_id"]
    ml_namespace = namespace.replace(".gold_qa_", ".ml_qa_")
    lease("acquire", identity, env)
    completed = False
    try:
        request = {**identity, "gold_namespace": namespace, "publication_id": publication["publication_id"],
                   "ml_namespace": ml_namespace, "datasets": config["datasets"]}
        _atomic_json(root / "request.json", request)
        identity_root = root / "java-identity"
        identity_root.mkdir()
        command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "512m",
                   "--class", "ie212.earthquake.spark.DatasetManifestJob",
                   "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(root / "request.json")]
        lease("assert", identity, env)
        with (root / "stdout.private.log").open("wb") as output, (root / "stderr.private.log").open("wb") as error:
            os.chmod(root / "stderr.private.log", 0o600)
            process = subprocess.run(command, env=java_identity(env, identity_root), stdout=output, stderr=error,
                                     timeout=1800, check=False)
        if process.returncode:
            raise RuntimeError("DS_BUILD_FAILED_LEASE_RETAINED")
        report = json.loads((root / "mld-01-report.json").read_text())
        if report.get("rerun_unchanged") is not True or len(report.get("manifests", [])) != 2:
            raise RuntimeError("DS_REPORT_INVALID")
        for entry in report["manifests"]:
            manifest = entry["manifest"]
            if manifest["gold_publication_id"] != publication["publication_id"] \
                    or manifest["gold_snapshot_id"] != publication["snapshot_bundle"]["event_current"]["snapshot_id"] \
                    or manifest["dataset_status"] != "BUILDING":
                raise RuntimeError("DS_MANIFEST_PIN_INVALID")
        completed = True
        report["ml_namespace"] = ml_namespace
        report["report_path"] = str(root / "mld-01-report.json")
        return report
    finally:
        # Remote SQL may still run after a JVM timeout: retain ownership on failure.
        if completed:
            lease("release", identity, env)


if __name__ == "__main__":
    try:
        report = run("/opt/pipeline/gold-publication-reference.json", "/opt/pipeline/dataset-config.json")
        print(json.dumps(report, sort_keys=True))
    except Exception:
        raise SystemExit("MLD01_QA_FAILED_CHECK_SCOPED_STAGING") from None
