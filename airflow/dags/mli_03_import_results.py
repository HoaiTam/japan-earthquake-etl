"""Control-plane DAG for importing an EXPORTED ML result bundle.

The request is deliberately small and exact.  Python owns staging, the shared
whole-run lease and process boundaries; Java/Spark owns Parquet validation and
Iceberg writes.  A successful runner must leave a bounded CANDIDATE report in
the staged run directory before the lease can be released.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
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
    if request.get("dataset_status") != "EXPORTED":
        raise Mli03Error("IMP_UNKNOWN_DATASET")
    if not isinstance(request.get("dataset_id"), str) or not request["dataset_id"]:
        raise Mli03Error("MLI03_DATASET_ID_REQUIRED")
    if not isinstance(request.get("artifact_root"), str) or not request["artifact_root"]:
        raise Mli03Error("MLI03_ARTIFACT_ROOT_REQUIRED")
    if not isinstance(request.get("journal_root"), str) or not request["journal_root"]:
        raise Mli03Error("MLI03_JOURNAL_ROOT_REQUIRED")
    if not isinstance(request.get("ml_namespace"), str) or not SAFE_NAMESPACE.fullmatch(request["ml_namespace"]):
        raise Mli03Error("MLI03_NAMESPACE_INVALID")
    candidates = request.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise Mli03Error("IMP_CANDIDATE_LINEAGE_REQUIRED")
    for candidate in candidates:
        if (not isinstance(candidate, dict)
                or any(not isinstance(candidate.get(key), str) or not candidate[key]
                       for key in ("mainshock_event_id", "candidate_event_id", "role"))):
            raise Mli03Error("IMP_CANDIDATE_LINEAGE_REQUIRED")


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


def run(request_path: str | Path, environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    request = _read_json(Path(request_path), MAX_REQUEST_BYTES)
    _validate_request(request)
    run_id = request["run_id"]
    root = Path(env.get("MLI_STAGING_ROOT", "/opt/pipeline/staging/ml-import")) / run_id
    staged_request = root / "request.json"
    identity = {"dag_id": "mli_03_import_results", "run_id": run_id}
    lease("acquire", identity, env)
    succeeded = False
    try:
        _atomic_json(staged_request, request)
        command_text = env.get("MLI_IMPORT_RUNNER_COMMAND", "").strip()
        if not command_text:
            raise Mli03Error("MLI03_IMPORT_RUNNER_COMMAND_REQUIRED")
        timeout = int(env.get("MLI_IMPORT_RUNNER_TIMEOUT_SECONDS", "3600"))
        if not 1 <= timeout <= 3600:
            raise Mli03Error("MLI03_RUNNER_TIMEOUT_INVALID")
        command = shlex.split(command_text)
        if not command:
            raise Mli03Error("MLI03_IMPORT_RUNNER_COMMAND_REQUIRED")
        identity_root = Path(tempfile.mkdtemp(prefix="mli03-identity-", dir=root))
        child_env = java_identity(env, identity_root)
        completed = subprocess.run(command + [str(staged_request)], env=child_env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   timeout=timeout, check=False)
        if completed.returncode != 0:
            raise Mli03Error("MLI03_IMPORT_RUNNER_FAILED")
        report_root = Path(request["journal_root"])
        report = _report(report_root)
        if report["experiment_run_id"] != request["experiment_run_id"] or report.get("dataset_id") != request["dataset_id"]:
            raise Mli03Error("MLI03_REPORT_IDENTITY_MISMATCH")
        lease("assert", identity, env)
        succeeded = True
        return {"dag_id": identity["dag_id"], "run_id": run_id, "status": "CANDIDATE",
                "report_path": str(report_root / "import-report.json"), "readback": report["readback"]}
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        raise Mli03Error("MLI03_IMPORT_RUNNER_FAILED") from error
    finally:
        if succeeded:
            lease("release", identity, env)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        raise SystemExit("MLI03_REQUEST_REQUIRED")
    try:
        print(json.dumps(run(sys.argv[1]), sort_keys=True))
    except Mli03Error as error:
        raise SystemExit(str(error)) from None
