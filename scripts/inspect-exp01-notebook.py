#!/usr/bin/env python3
"""EXP-01: Execute the thin harness smoke notebook in a fresh isolated environment; never edit tracked outputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def run_notebook_cells(notebook_path: Path, env_override: dict) -> dict:
    source_bytes = notebook_path.read_bytes()
    source_sha = hashlib.sha256(source_bytes).hexdigest()
    data = json.loads(source_bytes.decode("utf-8"))

    # Ensure no committed execution counts or outputs in tracked source
    for cell in data.get("cells", []):
        if cell.get("cell_type") == "code":
            if cell.get("outputs") or cell.get("execution_count") is not None:
                raise ValueError("EXP01_SOURCE_NOTEBOOK_MUST_BE_CLEARED")

    code_cells = [cell for cell in data.get("cells", []) if cell.get("cell_type") == "code"]

    # Try nbclient first if available
    nbclient_available = False
    try:
        import nbformat
        from nbclient import NotebookClient
        from jupyter_client import KernelManager
        from jupyter_client.kernelspec import KernelSpecManager
        nbclient_available = True
    except ImportError:
        nbclient_available = False

    executed_count = 0
    t0 = time.time()

    if nbclient_available:
        import nbformat
        from nbclient import NotebookClient
        from jupyter_client import KernelManager
        from jupyter_client.kernelspec import KernelSpecManager

        notebook = nbformat.reads(source_bytes.decode("utf-8"), as_version=4)
        with tempfile.TemporaryDirectory(prefix="exp01-kernel-") as temporary:
            kernel_dir = Path(temporary) / "exp01"
            kernel_dir.mkdir()
            (kernel_dir / "kernel.json").write_text(
                json.dumps({
                    "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                    "display_name": "EXP-01 isolated",
                    "language": "python"
                })
            )
            manager = KernelManager(kernel_name="exp01", kernel_spec_manager=KernelSpecManager(kernel_dirs=[temporary]))
            client = NotebookClient(notebook, km=manager, timeout=120, allow_errors=False, resources={"metadata": {"path": str(ROOT)}})
            try:
                client.execute(env=dict(os.environ, **env_override))
            finally:
                if manager.has_kernel:
                    manager.shutdown_kernel(now=True)
            executed_count = len(code_cells)
    else:
        # Direct execution in isolated namespace
        exec_globals = {
            "__name__": "__main__",
            "__file__": str(notebook_path),
        }
        old_env = os.environ.copy()
        try:
            os.environ.update(env_override)
            for idx, cell in enumerate(code_cells):
                source_code = "".join(cell.get("source", []))
                exec(compile(source_code, f"<cell_{idx}>", "exec"), exec_globals)
                executed_count += 1
        finally:
            os.environ.clear()
            os.environ.update(old_env)

    elapsed = time.time() - t0

    # Verify source file was not mutated
    if hashlib.sha256(notebook_path.read_bytes()).hexdigest() != source_sha:
        raise ValueError("EXP01_SOURCE_NOTEBOOK_MUTATED")

    return {
        "notebook_status": "RESTART_RUN_ALL_PASSED" if nbclient_available else "FALLBACK_EXEC_PASSED",
        "local_smoke_status": "PASSED" if nbclient_available else "FALLBACK_EXEC_WARNING",
        "colab_migration_allowed": bool(nbclient_available),
        "fresh_kernel_acceptance": bool(nbclient_available),
        "notebook_sha256": source_sha,
        "executed_code_cells": executed_count,
        "runtime_seconds": round(elapsed, 4),
        "python_version": sys.version.split()[0],
        "runner_mode": "nbclient" if nbclient_available else "isolated_exec",
        "source_notebook_outputs_cleared": True,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, default=None, help="Optional pilot bundle path")
    parser.add_argument("--report", type=Path, default=ROOT / "staging/exp01-notebook-inspection.json")
    args = parser.parse_args()

    source = ROOT / "notebooks/exp_01_harness_smoke.ipynb"
    env_override = {}
    if args.bundle and args.bundle.is_dir():
        env_override["PILOT_BUNDLE"] = str(args.bundle.resolve())

    result = run_notebook_cells(source, env_override)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
