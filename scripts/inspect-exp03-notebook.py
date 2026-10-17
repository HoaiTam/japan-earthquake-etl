#!/usr/bin/env python3
"""EXP-03: Execute the HDBSCAN adaptive notebook in a fresh isolated environment; never edit tracked outputs."""
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
                raise ValueError("EXP03_SOURCE_NOTEBOOK_MUST_BE_CLEARED")

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
        with tempfile.TemporaryDirectory(prefix="exp03-kernel-") as temporary:
            kernel_dir = Path(temporary) / "exp03"
            kernel_dir.mkdir()
            (kernel_dir / "kernel.json").write_text(
                json.dumps({
                    "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                    "display_name": "EXP-03 isolated",
                    "language": "python"
                })
            )
            manager = KernelManager(kernel_name="exp03", kernel_spec_manager=KernelSpecManager(kernel_dirs=[temporary]))
            client = NotebookClient(notebook, km=manager, timeout=180, allow_errors=False, resources={"metadata": {"path": str(ROOT)}})
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
        raise ValueError("EXP03_SOURCE_NOTEBOOK_MUTATED")

    return {
        "notebook_status": "RESTART_RUN_ALL_PASSED",
        "local_smoke_status": "PASSED",
        "colab_migration_allowed": True,
        "notebook_sha256": source_sha,
        "executed_code_cells": executed_count,
        "runtime_seconds": round(elapsed, 4),
        "python_version": sys.version.split()[0],
        "runner_mode": "nbclient" if nbclient_available else "isolated_exec",
        "source_notebook_outputs_cleared": True,
    }


def main():
    parser = argparse.ArgumentParser(description="Inspect and verify EXP-03 HDBSCAN adaptive notebook.")
    parser.add_argument("--notebook", default=str(ROOT / "notebooks/exp_03_adaptive_hdbscan.ipynb"), help="Path to notebook")
    parser.add_argument("--pilot-bundle", default=os.environ.get("PILOT_BUNDLE", ""), help="Path to pilot bundle directory")
    parser.add_argument("--output-json", default="", help="Optional output path for inspection JSON")
    args = parser.parse_args()

    notebook_path = Path(args.notebook).resolve()
    if not notebook_path.is_file():
        print(f"Error: Notebook not found: {notebook_path}", file=sys.stderr)
        sys.exit(1)

    env_override = {}
    if args.pilot_bundle:
        env_override["PILOT_BUNDLE"] = args.pilot_bundle

    try:
        report = run_notebook_cells(notebook_path, env_override)
        output_str = json.dumps(report, indent=2)
        print(output_str)

        if args.output_json:
            out_p = Path(args.output_json).resolve()
            out_p.parent.mkdir(parents=True, exist_ok=True)
            out_p.write_text(output_str + "\n", encoding="utf-8")

        sys.exit(0)
    except Exception as exc:
        print(f"ERROR: EXP-03 notebook inspection failed: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
