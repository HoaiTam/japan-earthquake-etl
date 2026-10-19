"""Opt-in fresh MLI-03 DAG.test against an already accepted synthetic QA bundle.

Never clears/retries an existing DagRun or marks a task successful. Storage
preparation is a separate command; this module has no build/start/cleanup path.
"""
import hashlib
import json
import os
from pathlib import Path
import uuid

from jma_backfill_runtime import _atomic_json
from mli_03_import_results import _request_document, _validate_request
from mli_import_qa import STAGING, validate_report


def validated_request(path):
    path = Path(path)
    if path.is_symlink() or not path.resolve().is_relative_to(STAGING.resolve()) or path.name != "import-request.json":
        raise RuntimeError("MLI03_DAG_QA_SCOPED_REQUEST_REQUIRED")
    request, digest = _request_document(path)
    _validate_request(request)
    report = json.loads((path.parent / "acceptance-report.json").read_text())
    validate_report(report, request["ml_namespace"])
    if report.get("lease_release", {}).get("status") != "RELEASED" or report.get("control_plane", {}).get("status") != "CANDIDATE":
        raise RuntimeError("MLI03_DAG_QA_STORAGE_ACCEPTANCE_REQUIRED")
    if (Path(request["artifact_root"]).resolve() != (path.parent / "valid").resolve()
            or Path(request["journal_root"]).resolve() != (path.parent / "journal").resolve()
            or not Path(request["artifact_root"]).resolve().is_relative_to(path.parent.resolve())
            or not Path(request["journal_root"]).resolve().is_relative_to(path.parent.resolve())
            or request["dataset_manifest_ref"] != "dataset.json"
            or request["dataset_manifest_sha256"] != hashlib.sha256((path.parent / "registry/dataset.json").read_bytes()).hexdigest()):
        raise RuntimeError("MLI03_DAG_QA_REQUEST_DRIFT")
    return request, digest, report


def run(path):
    request, digest, accepted = validated_request(path)
    root = Path(path).parent
    # Uses the same guard as ORC-07: refuse existing logical dates and active
    # runs BEFORE DAG.test (which otherwise clears by logical date internally).
    import pendulum
    from real_etl_acceptance import dag_test
    from mli_03_import_results import dag
    os.environ["MLI_DATASET_REGISTRY_ROOT"] = str(root / "registry")
    os.environ["MLI_IMPORT_RUNNER_COMMAND"] = "/opt/pipeline/bin/mli-import-runner"
    os.environ["MLI_SPARK_MASTER"] = "local[1]"
    dag_run, states = dag_test(dag, pendulum.now("UTC"), {"request_path": str(path)})
    if set(states) != set(dag.task_ids):
        raise RuntimeError("MLI03_DAG_QA_TASK_SET_MISMATCH")
    publication = json.loads((root / "journal/import-report.json").read_text())
    for key in ("membership_snapshot_id", "summary_snapshot_id", "experiment_snapshot_id", "readback"):
        if publication.get(key) != accepted["publication"][key]:
            raise RuntimeError("MLI03_DAG_QA_RERUN_CHANGED_PUBLICATION")
    report = {"task": "MLI-03", "status": "PASSED", "backend": "Airflow-DAG.test-Java-MinIO-Iceberg-Trino",
              "scheduled_run": False, "synthetic_component_qa": True, "scientific_approval": False,
              "dag_id": dag.dag_id, "dag_run_id": dag_run.run_id, "tasks": states,
              "namespace": request["ml_namespace"], "request_sha256": digest, "publication": publication,
              "completion_gate": "success", "publication_pins_unchanged": True}
    target = root / ("dag-acceptance-" + uuid.uuid4().hex + ".json")
    report["report_path"] = str(target)
    _atomic_json(target, report)
    return report


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        raise SystemExit("MLI03_DAG_QA_EXACT_REQUEST_REQUIRED")
    try:
        print(json.dumps(run(sys.argv[1]), sort_keys=True))
    except Exception:
        raise SystemExit("MLI03_DAG_QA_FAILED_CHECK_SCOPED_EVIDENCE_NO_AUTO_RELEASE") from None
