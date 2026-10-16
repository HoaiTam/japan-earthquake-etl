"""Control-plane DAG for importing an EXPORTED ML result bundle.

The request is deliberately small and exact.  Python owns staging, the shared
whole-run lease and process boundaries; Java/Spark owns Parquet validation and
Iceberg writes.  A successful runner must leave a bounded CANDIDATE report in
the staged run directory before the lease can be released.
"""

from __future__ import annotations

import json
import hashlib
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import uuid
from typing import Any, Mapping

from jma_backfill_runtime import _atomic_json

try:
    from resource_pilot import java_identity
except ModuleNotFoundError:  # pragma: no cover - Windows offline control-plane tests
    def java_identity(environment: Mapping[str, str], root: Path) -> dict[str, str]:
        return dict(environment)
from source_run_guard import lease


class Mli03Error(RuntimeError):
    """Fail-closed request, runner or readback error."""


MAX_REQUEST_BYTES = 64 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
SAFE_RUN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
SAFE_NAMESPACE = re.compile(r"[a-z][a-z0-9_]*\.ml_qa_[0-9a-f]{8,64}\Z")


def _read_json(path: Path, limit: int) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise Mli03Error("MLI03_REQUEST_NOT_BOUNDED")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Mli03Error("MLI03_REQUEST_INVALID_JSON") from error
    if not isinstance(value, dict):
        raise Mli03Error("MLI03_REQUEST_OBJECT_REQUIRED")
    return value


def _validate_request(request: Mapping[str, Any]) -> None:
    if request.get("dag_id") != "mli_03_import_results":
        raise Mli03Error("MLI03_DAG_ID_MISMATCH")
    run_id = request.get("run_id")
    if not isinstance(run_id, str) or not SAFE_RUN.fullmatch(run_id):
        raise Mli03Error("MLI03_RUN_ID_INVALID")
    experiment_run_id = request.get("experiment_run_id")
    if not isinstance(experiment_run_id, str) or not SAFE_RUN.fullmatch(experiment_run_id):
        raise Mli03Error("MLI03_EXPERIMENT_RUN_ID_INVALID")
    if not isinstance(request.get("dataset_id"), str) or not request["dataset_id"]:
        raise Mli03Error("MLI03_DATASET_ID_REQUIRED")
    manifest_ref = request.get("dataset_manifest_ref")
    manifest_sha = request.get("dataset_manifest_sha256")
    if (not isinstance(manifest_ref, str) or not manifest_ref or Path(manifest_ref).is_absolute()
            or ".." in Path(manifest_ref).parts or not re.fullmatch(r"[A-Za-z0-9_./-]{1,256}", manifest_ref)
            or not isinstance(manifest_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", manifest_sha)):
        raise Mli03Error("MLI03_DATASET_REFERENCE_INVALID")
    # Status and candidate IDs are resolved from the trusted registry by Java.
    if "dataset_status" in request or "candidates" in request:
        raise Mli03Error("MLI03_CALLER_LINEAGE_NOT_ALLOWED")
    if any(key in request for key in ("_mli03_lease_owner", "_mli03_request_sha256")):
        raise Mli03Error("MLI03_RESERVED_CONTEXT_FIELD")
    if not isinstance(request.get("artifact_root"), str) or not request["artifact_root"]:
        raise Mli03Error("MLI03_ARTIFACT_ROOT_REQUIRED")
    if not isinstance(request.get("journal_root"), str) or not request["journal_root"]:
        raise Mli03Error("MLI03_JOURNAL_ROOT_REQUIRED")
    if not isinstance(request.get("ml_namespace"), str) or not SAFE_NAMESPACE.fullmatch(request["ml_namespace"]):
        raise Mli03Error("MLI03_NAMESPACE_INVALID")


def _report(report_root: Path) -> dict[str, Any]:
    report = _read_json(report_root / "import-report.json", MAX_REPORT_BYTES)
    if report.get("experiment_status") != "CANDIDATE":
        raise Mli03Error("MLI03_PUBLICATION_NOT_CANDIDATE")
    if not isinstance(report.get("experiment_run_id"), str):
        raise Mli03Error("MLI03_REPORT_IDENTITY_MISMATCH")
    readback = report.get("readback")
    if not isinstance(readback, dict) or any(
            type(readback.get(name)) is not int or readback[name] <= 0
            for name in ("sequence_membership", "sequence_summary", "experiment_run")):
        raise Mli03Error("MLI03_READBACK_REQUIRED")
    return report


def _request_document(path: Path) -> tuple[dict[str, Any], str]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_REQUEST_BYTES:
        raise Mli03Error("MLI03_REQUEST_NOT_BOUNDED")
    try:
        raw = path.read_bytes()
        request = json.loads(raw)
    except (OSError, ValueError) as error:
        raise Mli03Error("MLI03_REQUEST_INVALID_JSON") from error
    if not isinstance(request, dict):
        raise Mli03Error("MLI03_REQUEST_OBJECT_REQUIRED")
    return request, hashlib.sha256(raw).hexdigest()


def _owner(dag_id: Any, run_id: Any) -> dict[str, str]:
    if not isinstance(dag_id, str) or not dag_id or len(dag_id) > 250:
        raise Mli03Error("MLI03_DAGRUN_IDENTITY_REQUIRED")
    if not isinstance(run_id, str) or not run_id or len(run_id) > 250:
        raise Mli03Error("MLI03_DAGRUN_IDENTITY_REQUIRED")
    return {"dag_id": dag_id, "run_id": run_id}


def _airflow_owner(context: Mapping[str, Any]) -> dict[str, str]:
    dag_run = context.get("dag_run")
    dag_id = getattr(dag_run, "dag_id", None) or context.get("dag_id")
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
    return _owner(dag_id, run_id)


def _owner_key(owner: Mapping[str, str]) -> str:
    encoded = json.dumps(dict(owner), sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def resolve_request_context(request_path: str | Path, owner: Mapping[str, str],
                            environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Pin one validated request to a real DagRun owner for every downstream task."""
    env = dict(os.environ if environment is None else environment)
    owner = _owner(owner.get("dag_id"), owner.get("run_id"))
    source = Path(request_path)
    request, request_sha256 = _request_document(source)
    _validate_request(request)
    context_root = Path(env.get("MLI_STAGING_ROOT", "/opt/pipeline/staging/ml-import")) / ".mli03-contexts" / _owner_key(owner)
    context_path = context_root / "resolved-context.json"
    source_path = str(source.resolve())
    document = {"version": "mli03-resolved-context-v1", "request": request,
                "request_sha256": request_sha256, "request_path": source_path, "lease_owner": owner}
    if context_path.exists():
        existing = _read_json(context_path, MAX_REQUEST_BYTES * 2)
        if (existing.get("lease_owner") != owner or existing.get("request_sha256") != request_sha256
                or existing.get("request_path") != source_path):
            raise Mli03Error("MLI03_CONTEXT_ALREADY_PINNED")
    else:
        _atomic_json(context_path, document)
    context_bytes = context_path.read_bytes()
    return {"context_path": str(context_path), "context_sha256": hashlib.sha256(context_bytes).hexdigest(),
            "request_path": source_path, "request_sha256": request_sha256, "lease_owner": owner}


def _load_resolved_context(descriptor: Mapping[str, Any], environment: Mapping[str, str],
                           expected_owner: Mapping[str, str] | None = None) -> dict[str, Any]:
    if not isinstance(descriptor, Mapping):
        raise Mli03Error("MLI03_RESOLVED_CONTEXT_REQUIRED")
    root = Path(environment.get("MLI_STAGING_ROOT", "/opt/pipeline/staging/ml-import")).resolve()
    path = Path(str(descriptor.get("context_path", "")))
    if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root):
        raise Mli03Error("MLI03_RESOLVED_CONTEXT_INVALID")
    raw = path.read_bytes()
    if len(raw) > MAX_REQUEST_BYTES * 2 or hashlib.sha256(raw).hexdigest() != descriptor.get("context_sha256"):
        raise Mli03Error("MLI03_RESOLVED_CONTEXT_CHECKSUM_MISMATCH")
    try:
        document = json.loads(raw)
    except ValueError as error:
        raise Mli03Error("MLI03_RESOLVED_CONTEXT_INVALID") from error
    if (not isinstance(document, dict) or document.get("version") != "mli03-resolved-context-v1"
            or document.get("lease_owner") != descriptor.get("lease_owner")
            or document.get("request_sha256") != descriptor.get("request_sha256")
            or document.get("request_path") != descriptor.get("request_path")):
        raise Mli03Error("MLI03_RESOLVED_CONTEXT_IDENTITY_MISMATCH")
    owner = _owner(document["lease_owner"].get("dag_id"), document["lease_owner"].get("run_id"))
    if expected_owner is not None and owner != dict(expected_owner):
        raise Mli03Error("MLI03_DAGRUN_IDENTITY_DRIFT")
    source = Path(document["request_path"])
    if source.is_symlink() or not source.is_file() or source.stat().st_size > MAX_REQUEST_BYTES:
        raise Mli03Error("MLI03_REQUEST_CHANGED_AFTER_RESOLVE")
    if hashlib.sha256(source.read_bytes()).hexdigest() != document["request_sha256"]:
        raise Mli03Error("MLI03_REQUEST_CHANGED_AFTER_RESOLVE")
    request = document.get("request")
    if not isinstance(request, dict):
        raise Mli03Error("MLI03_RESOLVED_CONTEXT_INVALID")
    _validate_request(request)
    return {"request": request, "lease_owner": owner, "request_sha256": document["request_sha256"],
            "context_path": str(path), "context_sha256": descriptor["context_sha256"]}


def _release_owned(owner: Mapping[str, str], environment: Mapping[str, str]) -> dict[str, str]:
    owner = _owner(owner.get("dag_id"), owner.get("run_id"))
    held = lease("assert", owner, environment)
    if held != {"status": "HELD", **owner}:
        raise Mli03Error("MLI03_LEASE_OWNER_MISMATCH")
    released = lease("release", owner, environment)
    if released != {"status": "RELEASED", **owner}:
        raise Mli03Error("MLI03_LEASE_RELEASE_UNCONFIRMED")
    return released


def _execute_import(request: Mapping[str, Any], lease_owner: Mapping[str, str], request_sha256: str,
                    environment: Mapping[str, str], manage_lease: bool = True,
                    context_sha256: str | None = None) -> dict[str, Any]:
    request = dict(request)
    _validate_request(request)
    owner = _owner(lease_owner.get("dag_id"), lease_owner.get("run_id"))
    if manage_lease:
        acquired = lease("acquire", owner, environment)
        if acquired != {"status": "ACQUIRED", **owner}:
            raise Mli03Error("MLI03_LEASE_ACQUIRE_UNCONFIRMED")
    else:
        held = lease("assert", owner, environment)
        if held != {"status": "HELD", **owner}:
            raise Mli03Error("MLI03_LEASE_OWNER_MISMATCH")
    root = Path(environment.get("MLI_STAGING_ROOT", "/opt/pipeline/staging/ml-import")) / _owner_key(owner)
    root.mkdir(parents=True, exist_ok=True)
    staged_request = root / "request.json"
    staged = {**request, "_mli03_lease_owner": owner, "_mli03_request_sha256": request_sha256}
    _atomic_json(staged_request, staged)
    command_text = environment.get("MLI_IMPORT_RUNNER_COMMAND", "").strip()
    if not command_text:
        raise Mli03Error("MLI03_IMPORT_RUNNER_COMMAND_REQUIRED")
    try:
        timeout = int(environment.get("MLI_IMPORT_RUNNER_TIMEOUT_SECONDS", "3600"))
    except ValueError as error:
        raise Mli03Error("MLI03_RUNNER_TIMEOUT_INVALID") from error
    if not 1 <= timeout <= 3600:
        raise Mli03Error("MLI03_RUNNER_TIMEOUT_INVALID")
    command = shlex.split(command_text)
    if not command:
        raise Mli03Error("MLI03_IMPORT_RUNNER_COMMAND_REQUIRED")
    identity_root = Path(tempfile.mkdtemp(prefix="mli03-identity-", dir=root))
    child_env = java_identity(environment, identity_root)
    try:
        completed = subprocess.run(command + [str(staged_request)], env=child_env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   timeout=timeout, check=False)
        if completed.returncode != 0:
            raise Mli03Error("MLI03_IMPORT_RUNNER_FAILED")
        report_root = Path(request["journal_root"])
        report = _report(report_root)
        if report["experiment_run_id"] != request["experiment_run_id"] or report.get("dataset_id") != request["dataset_id"]:
            raise Mli03Error("MLI03_REPORT_IDENTITY_MISMATCH")
        held = lease("assert", owner, environment)
        if held != {"status": "HELD", **owner}:
            raise Mli03Error("MLI03_LEASE_OWNER_MISMATCH")
        release = _release_owned(owner, environment) if manage_lease else None
        return {"dag_id": request["dag_id"], "run_id": request["run_id"],
                "experiment_run_id": request["experiment_run_id"], "dataset_id": request["dataset_id"],
                "status": "CANDIDATE", "report_path": str(report_root / "import-report.json"),
                "readback": report["readback"], "lease_owner": owner,
                "request_sha256": request_sha256, "context_sha256": context_sha256, "release": release}
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        raise Mli03Error("MLI03_IMPORT_RUNNER_FAILED") from error


def run(request_path: str | Path, environment: Mapping[str, str] | None = None,
        manage_lease: bool = True) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    request, request_sha256 = _request_document(Path(request_path))
    _validate_request(request)
    owner = _owner(request["dag_id"], request["run_id"])
    return _execute_import(request, owner, request_sha256, env, manage_lease)


def _task_context(context: Mapping[str, Any], environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    descriptor = context["ti"].xcom_pull(task_ids="resolve_request")
    return _load_resolved_context(descriptor, env, _airflow_owner(context))


def completion_result(pinned: Mapping[str, Any], result: Any, release: Any) -> dict[str, Any]:
    owner = pinned["lease_owner"]
    request = pinned["request"]
    if (not isinstance(result, dict) or result.get("status") != "CANDIDATE"
            or result.get("run_id") != request["run_id"]
            or result.get("experiment_run_id") != request["experiment_run_id"]
            or result.get("dataset_id") != request["dataset_id"]
            or result.get("lease_owner") != owner
            or result.get("request_sha256") != pinned["request_sha256"]):
        raise Mli03Error("MLI03_COMPLETION_GATE_FAILED")
    if not isinstance(release, dict) or release != {"status": "RELEASED", **owner}:
        raise Mli03Error("MLI03_LEASE_RELEASE_UNCONFIRMED")
    return {"status": "COMPLETE", "lease_owner": owner,
            "experiment_run_id": request["experiment_run_id"]}


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("MLI03_REQUEST_REQUIRED")
    try:
        print(json.dumps(run(sys.argv[1]), sort_keys=True))
    except Mli03Error as error:
        raise SystemExit(str(error)) from None


# Airflow is optional for offline/unit use.  The module keeps the runtime
# helper above importable without the scheduler SDK, while a real deployment
# receives a manual, single-active-run DAG with strict downstream ordering.
try:  # pragma: no cover - exercised by the Airflow image, not Windows unit tests
    from airflow import DAG
    from airflow.operators.python import PythonOperator

    def _request_from_conf(**context):
        conf = (context.get("dag_run").conf or {}) if context.get("dag_run") else {}
        request_path = conf.get("request_path") or os.environ.get("MLI_IMPORT_REQUEST")
        if not request_path:
            raise Mli03Error("MLI03_REQUEST_REQUIRED")
        return resolve_request_context(request_path, _airflow_owner(context))

    def _assert_resolved_context(**context):
        pinned = _task_context(context)
        owner = pinned["lease_owner"]
        held = lease("assert", owner)
        if held != {"status": "HELD", **owner}:
            raise Mli03Error("MLI03_LEASE_OWNER_MISMATCH")
        return {"request_sha256": pinned["request_sha256"], "lease_owner": owner}

    def _acquire_from_request(**context):
        pinned = _task_context(context)
        owner = pinned["lease_owner"]
        result = lease("acquire", owner)
        if result != {"status": "ACQUIRED", **owner}:
            raise Mli03Error("MLI03_LEASE_ACQUIRE_UNCONFIRMED")
        return result

    def _run_from_context(**context):
        pinned = _task_context(context)
        return _execute_import(pinned["request"], pinned["lease_owner"], pinned["request_sha256"],
                               os.environ, manage_lease=False, context_sha256=pinned["context_sha256"])

    def _release_from_request(**context):
        pinned = _task_context(context)
        return _release_owned(pinned["lease_owner"], os.environ)

    def _completion_gate(**context):
        pinned = _task_context(context)
        result = context["ti"].xcom_pull(task_ids="publish_readback")
        release = context["ti"].xcom_pull(task_ids="release_lease")
        return completion_result(pinned, result, release)

    with DAG("mli_03_import_results", schedule=None, catchup=False, max_active_runs=1,
             default_args={"retries": 0}, tags=["ml", "import", "manual"]) as dag:
        resolve_request = PythonOperator(task_id="resolve_request", python_callable=_request_from_conf)
        acquire_lease = PythonOperator(task_id="acquire_lease", python_callable=_acquire_from_request)
        stage = PythonOperator(task_id="stage", python_callable=_assert_resolved_context)
        resolve_validate = PythonOperator(task_id="resolve_dataset_validate", python_callable=_assert_resolved_context)
        commit_results = PythonOperator(task_id="commit_results", python_callable=_assert_resolved_context)
        trino_verify = PythonOperator(task_id="trino_verify", python_callable=_assert_resolved_context)
        publish_readback = PythonOperator(task_id="publish_readback", python_callable=_run_from_context)
        release_lease = PythonOperator(task_id="release_lease", python_callable=_release_from_request, trigger_rule="all_success")
        completion_gate = PythonOperator(task_id="completion_gate", python_callable=_completion_gate, trigger_rule="all_success")
        resolve_request >> acquire_lease >> stage >> resolve_validate >> commit_results >> trino_verify >> publish_readback >> release_lease >> completion_gate
except (ModuleNotFoundError, ImportError):  # pragma: no cover - expected in local Windows tests
    dag = None
