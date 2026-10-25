#!/usr/bin/env python3
"""Package a verified development pilot plus its offline loader and instructions; never a research release."""
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.dont_write_bytecode = True
sys.path.insert(0, str(ROOT / "notebooks"))
from pilot_bundle import load_bundle

spec = importlib.util.spec_from_file_location("team_verifier", ROOT / "scripts/verify-team-pilot.py")
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


def pack(bundle, output):
    bundle, output = Path(bundle), Path(output)
    verified = load_bundle(bundle)
    summary = verified["summary"]
    # Source allowlist is exact: not a repo snapshot, .env, private logs or arbitrary paths.
    payload = {"bundle/" + p.name: p.read_bytes() for p in bundle.iterdir()}
    payload.update({
        "README.md": (ROOT / "docs/runbooks/DAT-03_PILOT_TEAM_README.md").read_bytes(),
        "requirements.txt": b"pyarrow==21.0.0\n",
        "verify_dataset.py": (ROOT / "scripts/verify-team-pilot.py").read_bytes(),
        "tools/pilot_bundle.py": (ROOT / "notebooks/pilot_bundle.py").read_bytes(),
    })
    if set(payload) != verifier.FILES:
        raise ValueError("TEAM_SOURCE_FILE_SET_MISMATCH")
    # Validate the captured bytes, not only the source before it was read.
    # A concurrently changed source must not become a freshly rehashed "verified" kit.
    with tempfile.TemporaryDirectory(prefix="dat03-pack-check-") as temporary:
        captured = Path(temporary)
        for name, data in payload.items():
            if name.startswith("bundle/"):
                target = captured / name.removeprefix("bundle/")
                target.write_bytes(data)
        captured_summary = load_bundle(captured)["summary"]
        if captured_summary != summary:
            raise ValueError("TEAM_SOURCE_CHANGED_DURING_PACK")
    handoff = {
        "version": "dat03-team-pilot-v1", "artifact_kind": "DEVELOPMENT_PILOT",
        "allowed_usage": "DEVELOPMENT_ONLY", "research_ready": False,
        "pilot_id": summary["pilot_id"], "feature_count": summary["features"],
        "mainshock_count": summary["mainshocks"], "coverage_status": summary["coverage_status"],
        "source_dataset_id": verified["manifest"]["source_dataset_id"],
        "recipients": list(verifier.ROSTER), "scientific_approval": False,
        "files": {name: {"bytes": len(data), "sha256": verifier.sha(data)}
                  for name, data in sorted(payload.items())},
    }
    metadata = (json.dumps(handoff, indent=2, sort_keys=True) + "\n").encode()
    payload["handoff.json"] = metadata
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, data in sorted(payload.items()):
            info = zipfile.ZipInfo(name, date_time=(2000, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100600 << 16
            archive.writestr(info, data)
    data = memory.getvalue()
    if output.is_symlink():
        raise ValueError("TEAM_OUTPUT_SYMLINK_REJECTED")
    reused = output.exists()
    if reused:
        if output.read_bytes() != data:
            raise ValueError("TEAM_OUTPUT_CONFLICT_NO_OVERWRITE")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as target:
            target.write(data)
    return {
        "status": "TEAM_PILOT_PACKED", "pilot_id": summary["pilot_id"],
        "bytes": len(data), "zip_sha256": hashlib.sha256(data).hexdigest(),
        "handoff_sha256": verifier.sha(metadata), "files": len(payload),
        "feature_count": summary["features"], "mainshock_count": summary["mainshocks"],
        "coverage_status": summary["coverage_status"], "research_ready": False, "reused": reused,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    print(json.dumps(pack(args.bundle, args.output), indent=2, sort_keys=True))
