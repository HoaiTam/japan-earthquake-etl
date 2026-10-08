"""Fail-closed cross-DAG source lease for the shared LocalExecutor staging volume.

flock protects atomic lease updates; the persisted owner protects the WHOLE run,
not just a task/process. No expiration, takeover or automatic deletion on failure.
"""
import fcntl
import json
import os
from pathlib import Path

from jma_backfill_runtime import _atomic_json


class SourceRunBusy(RuntimeError):
    pass


def owner(context):
    dag = context.get("dag")
    dag_id = context.get("dag_id") or getattr(dag, "dag_id", None)
    run_id = context.get("run_id")
    if not dag_id or not run_id:
        raise ValueError("source lease requires dag_id and run_id")
    return {"dag_id": str(dag_id), "run_id": str(run_id)}


def lease(action, identity, environment=None):
    env = os.environ if environment is None else environment
    if action not in {"acquire", "assert", "release"} or set(identity) != {"dag_id", "run_id"}:
        raise ValueError("invalid source lease request")
    root = Path(env.get("SOURCE_GUARD_ROOT", "/opt/pipeline/staging/source-guard"))
    root.mkdir(parents=True, exist_ok=True)
    target = root / "owner.json"
    with (root / "mutex.lock").open("a") as mutex:
        fcntl.flock(mutex, fcntl.LOCK_EX)
        # Corrupt/unreadable state fails closed; never silently remove another run.
        current = json.loads(target.read_text()) if target.exists() else None
        if action == "assert":
            if current != identity:
                raise SourceRunBusy("SOURCE_LEASE_NOT_HELD: rerun acquisition before source tasks")
            return {"status": "HELD", **identity}
        if action == "acquire":
            if current is not None and current != identity:
                raise SourceRunBusy("SOURCE_RUN_BUSY: daily/backfill must not overlap")
            _atomic_json(target, identity)
            return {"status": "ACQUIRED", **identity}
        if current == identity:
            target.unlink()  # Only our tiny lease, not staging data or publication pointers.
        return {"status": "RELEASED" if current in (None, identity) else "NOT_OWNER", **identity}
