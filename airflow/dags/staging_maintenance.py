"""Read-only exact-run staging diagnosis. Never implements TTL deletion or lease takeover."""
import argparse
import json
import os
from pathlib import Path

import run_observability as obs
from run_summary_cli import inspect_summary


def preview(root, dag_id, run_id, environment=None):
    env = os.environ if environment is None else environment
    seed = {"dag_id": dag_id, "run_id": obs.label(run_id)}
    folder = obs._root(seed, {"RUN_SUMMARY_ROOT": root})
    obs.require(folder.is_dir() and not folder.is_symlink(), "SUMMARY_NOT_FOUND")
    for item in (folder / "state.json", folder / "run_summary.json", folder / ".lock", folder / "events"):
        obs.require(not item.is_symlink(), "INVALID_MAINTENANCE_TARGET")
    summary = inspect_summary(dag_id, run_id, root)
    guard = Path(env.get("SOURCE_GUARD_ROOT", "/opt/pipeline/staging/source-guard")) / "owner.json"
    # Presence, including corrupt state, blocks any proposed maintenance; do not dump an owner file.
    busy = guard.exists() or guard.is_symlink()
    terminal = summary["status"] != "RUNNING"
    return {"task": "ORC-05", "mode": "preview_only", "dag_id": dag_id, "run_id": run_id,
            "status": summary["status"], "exact_summary_folder": str(folder),
            "terminal": terminal, "source_lease_present": busy,
            "automatic_cleanup_allowed": False, "mutation_performed": False,
            "recommendation": "RETAIN_ACTIVE_OR_RECOVERY_STATE" if busy or not terminal else
                              "RETAIN_AUDIT_REVIEW_EXACT_TEMP_FILES_SEPARATELY",
            "protected": ["Bronze", "Silver", "Gold", "Iceberg metadata", "operation pins",
                          "source receipts/cache", "run summaries/journal", "source lease"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(preview(args.root, args.dag_id, args.run_id), sort_keys=True))
    except Exception:
        raise SystemExit("ORC05_MAINTENANCE_PREVIEW_FAILED") from None
