#!/usr/bin/env python3
"""Build an explicit EXP-01 handoff after a real fresh local kernel, or run it in Colab."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "notebooks"))
import colab_runtime as cr


def local_pack(pilot_zip, output, recipient):
    pilot_zip, output = cr.safe(pilot_zip), cr.safe(output)
    cr.require(recipient in cr.RECIPIENTS and not output.exists(), "EXP01_NEW_LOCAL_WORK_REQUIRED")
    # Fail before executing notebook; never let its synthetic fixture fallback run.
    cr.require(cr.sha(pilot_zip) == cr.PILOT["zip_sha256"], "EXP01_REAL_PILOT_ZIP_REQUIRED")
    env = cr.environment(ROOT); identity = cr.source_identity(ROOT)
    output.mkdir(parents=True)
    child_env = {"EXP01_LOCAL_WORK": str(output), "EXP01_PILOT_ZIP": str(pilot_zip), "EXP01_RECIPIENT": recipient}
    inspector = cr.module(ROOT, "scripts/inspect-exp02-notebook.py", "exp01_nbclient_runner")
    inspection = inspector.run_notebook_cells(ROOT / "notebooks/exp_01_portability_local.ipynb", child_env)
    cr.require(inspection["runner_mode"] == "nbclient" and inspection["fresh_kernel_acceptance"] is True,
               "EXP01_NBCLIENT_FRESH_KERNEL_REQUIRED")
    report = output / "local-smoke-report.json"
    gate = {"version": cr.VERSION, "source": identity, "pilot": cr.PILOT, "environment": env,
            "local_smoke_sha256": cr.sha(report), "inspection": inspection,
            "git_base_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "source_identity_is_content_hash": True, "research_ready": False}
    cr.validate_gate(ROOT, gate, report, cr.sha(report), env)
    # Verify exact result bytes/configs before shipping local parity references.
    for algo in cr.MODELS: cr.verify_result(output / "baseline" / algo.lower(), algo, identity["sha256"])
    payload = output / "handoff"; payload.mkdir()
    files = {"code/"+n: ROOT / n for n in cr.SOURCES}
    cr.save(output / "local-gate.json", gate)
    files.update({"pilot.zip": pilot_zip, "local-gate.json": output / "local-gate.json", "local-smoke-report.json": report})
    files.update({"baseline/"+a.lower()+"/"+n: output / "baseline" / a.lower() / n
                  for a in cr.MODELS for n in cr.ARTIFACTS})
    from experiment_harness import validate_safe_metadata
    for name, path in files.items():
        if path.suffix in (".json", ".ipynb"): validate_safe_metadata(cr.load(path))
        target = payload / name; target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
        cr.require(cr.sha(target) == cr.sha(path), "EXP01_SOURCE_CHANGED_DURING_PACK")
    cr.require(cr.source_identity(ROOT) == identity, "EXP01_SOURCE_CHANGED_DURING_LOCAL_RUN")
    manifest = {"version": cr.VERSION, "source": identity, "pilot": cr.PILOT,
                "files": {n: cr.sha(payload / n) for n in sorted(files)}, "research_ready": False}
    cr.save(payload / "handoff.json", manifest); cr.verify_handoff(payload)
    archive = output / "exp01-colab-handoff.zip"
    with zipfile.ZipFile(archive, "x", compression=zipfile.ZIP_DEFLATED) as z:
        for name in sorted({*files, "handoff.json"}): z.write(payload / name, name)
    proof = {"status": "LOCAL_GATE_AND_HANDOFF_VERIFIED", "version": cr.VERSION,
             "zip_sha256": cr.sha(archive), "zip_bytes": archive.stat().st_size,
             "source_sha256": identity["sha256"], "local_smoke_sha256": cr.sha(report),
             "inspection": inspection, "pilot": cr.PILOT, "colab_executed": False, "research_ready": False}
    cr.save(output / "handoff-receipt.json", proof)
    print(json.dumps(proof, indent=2))
    return proof


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("local-pack", "run", "copy")); p.add_argument("--pilot-zip", type=Path)
    p.add_argument("--handoff-root", type=Path); p.add_argument("--output", type=Path, required=True)
    p.add_argument("--recipient", choices=sorted(cr.RECIPIENTS), default="HoaiTam")
    p.add_argument("--destination", type=Path); a = p.parse_args()
    if a.mode == "local-pack": local_pack(a.pilot_zip, a.output, a.recipient)
    elif a.mode == "run": print(json.dumps(cr.run_cloud(a.handoff_root, a.output, a.recipient), indent=2))
    else: print(json.dumps(cr.output_copy(a.output, a.destination, a.handoff_root), indent=2))


if __name__ == "__main__": main()
