#!/usr/bin/env python3
"""COL-02: fresh local handoff, actual Colab sweep, independent output readback."""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "notebooks"))
import colab_runtime as cr
import col02_runtime as c


def local_pack(pilot_zip, output, recipient, baseline_handoff, baseline_output):
    output = cr.safe(output)
    cr.require(not output.exists(), "COL02_NEW_LOCAL_WORK_REQUIRED")
    cr.require(cr.sha(pilot_zip) == cr.PILOT["zip_sha256"], "COL02_REAL_PILOT_REQUIRED")
    env, source = cr.environment(ROOT), c.identity(ROOT)
    output.mkdir(parents=True)
    upstream = c.verify_upstream(ROOT, baseline_handoff, baseline_output, output)
    inspector = cr.module(ROOT, "scripts/inspect-exp02-notebook.py", "col02_nbclient")
    inspection = inspector.run_notebook_cells(ROOT / "notebooks/col_02_portability_local.ipynb", {
        "COL02_ROOT": str(ROOT), "COL02_WORK": str(output / "local"),
        "COL02_PILOT_ZIP": str(cr.safe(pilot_zip)), "COL02_RECIPIENT": recipient,
    })
    cr.require(inspection["runner_mode"] == "nbclient" and inspection["fresh_kernel_acceptance"] is True,
               "COL02_FRESH_LOCAL_GATE_REQUIRED")
    report = output / "local/sweep-report.json"
    gate = {"version": c.VERSION, "source": source, "inspection": inspection,
            "report_sha256": cr.sha(report), "upstream_defaults": upstream,
            "git_base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "source_identity_is_content_hash": True, **c.FLAGS}
    cr.save(output / "local-gate.json", gate)
    files = {"code/" + n: ROOT / n for n in c.SOURCES}
    files.update({"pilot.zip": Path(pilot_zip), "local-gate.json": output / "local-gate.json",
                  "local-sweep-report.json": report})
    files.update({name: output / "local/results" / name.removeprefix("reference/") for name in c.result_files("reference")})
    files["reference/sweep_results_matrix.json"] = output / "local/portable-matrix.json"
    payload = output / "handoff"
    payload.mkdir()
    from experiment_harness import validate_safe_metadata
    for name, path in sorted(files.items()):
        if path.suffix in {".json", ".ipynb"}: validate_safe_metadata(cr.load(path))
        target = payload / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(cr.safe(path), target)
        cr.require(cr.sha(path) == cr.sha(target), "COL02_COPY_BYTES_CHANGED")
    cr.require(c.identity(ROOT) == source, "COL02_SOURCE_CHANGED_DURING_LOCAL_GATE")
    manifest = {"version": c.VERSION, "source": source, "pilot": cr.PILOT,
                "files": c.file_map(payload), **c.FLAGS}
    cr.save(payload / "handoff.json", manifest)
    c.verify_handoff(payload)
    c.validate_gate(payload, env)
    archive = output / "col02-colab-handoff.zip"
    c.archive_output(payload, archive)
    proof = {"status": "LOCAL_COL02_HANDOFF_VERIFIED", "zip_sha256": cr.sha(archive),
             "zip_bytes": archive.stat().st_size, "source_sha256": source["sha256"],
             "config_sha256": c.config_pins(ROOT), "inspection": inspection,
             "colab_executed": False, "upstream_defaults": upstream, **c.FLAGS}
    cr.save(output / "handoff-receipt.json", proof)
    return proof


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("local-pack", "run", "readback"))
    p.add_argument("--pilot-zip", type=Path)
    p.add_argument("--baseline-handoff", type=Path)
    p.add_argument("--baseline-output", type=Path)
    p.add_argument("--handoff-root", type=Path)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--recipient", choices=sorted(cr.RECIPIENTS), default="HoaiTam")
    a = p.parse_args()
    if a.mode == "local-pack":
        if not all((a.pilot_zip, a.baseline_handoff, a.baseline_output)):
            p.error("local-pack requires --pilot-zip, --baseline-handoff and --baseline-output")
        result = local_pack(a.pilot_zip, a.output, a.recipient, a.baseline_handoff, a.baseline_output)
    elif not a.handoff_root:
        p.error("--handoff-root is required")
    elif a.mode == "run": result = c.run_cloud(a.handoff_root, a.output, a.recipient)
    else: result = c.readback(a.handoff_root, a.output)
    print(json.dumps(result, indent=2))


if __name__ == "__main__": main()
