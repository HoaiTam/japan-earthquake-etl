#!/usr/bin/env python3
"""Execute the thin inspection notebook in a fresh local kernel; never edit its tracked outputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
from importlib.metadata import version
import nbformat
from nbclient import NotebookClient
from jupyter_client import KernelManager
from jupyter_client.kernelspec import KernelSpecManager

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--report", type=Path, default=ROOT / "staging/dat02-notebook-inspection.json")
    args = parser.parse_args()
    sys.path.insert(0, str(ROOT / "notebooks"))
    from pilot_bundle import load_bundle
    bundle_path = args.bundle.resolve(strict=True)
    before = load_bundle(bundle_path)
    source = ROOT / "notebooks/dat_02_inspection.ipynb"
    source_sha = hashlib.sha256(source.read_bytes()).hexdigest()
    notebook = nbformat.read(source, as_version=4)
    if any(c.get("outputs") or c.get("execution_count") is not None for c in notebook.cells if c.cell_type == "code"):
        raise ValueError("DAT02_SOURCE_NOTEBOOK_MUST_BE_CLEARED")
    with tempfile.TemporaryDirectory(prefix="dat02-kernel-") as temporary:
        kernel_dir = Path(temporary) / "dat02"; kernel_dir.mkdir()
        (kernel_dir / "kernel.json").write_text(json.dumps({"argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                                                          "display_name": "DAT-02 isolated", "language": "python"}))
        manager = KernelManager(kernel_name="dat02", kernel_spec_manager=KernelSpecManager(kernel_dirs=[temporary]))
        client = NotebookClient(notebook, km=manager, timeout=120, allow_errors=False, resources={"metadata": {"path": str(ROOT)}})
        try:
            client.execute(env=dict(os.environ, PILOT_BUNDLE=str(bundle_path)))
        finally:
            if manager.has_kernel:
                manager.shutdown_kernel(now=True)
    if hashlib.sha256(source.read_bytes()).hexdigest() != source_sha:
        raise ValueError("DAT02_SOURCE_NOTEBOOK_CHANGED")
    after = load_bundle(bundle_path)
    assert before["summary"] == after["summary"]
    result = dict(after["summary"], notebook_status="RESTART_RUN_ALL_PASSED", notebook_sha256=source_sha,
                  executed_code_cells=sum(c.cell_type == "code" for c in notebook.cells),
                  python=sys.version.split()[0], dependencies={p: version(p) for p in ["pyarrow", "nbformat", "nbclient", "ipykernel", "jupyterlab"]},
                  bundle_checksums_sha256=hashlib.sha256((bundle_path / "checksums.sha256").read_bytes()).hexdigest(),
                  network_access_required=False, source_notebook_outputs_cleared=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__": main()
