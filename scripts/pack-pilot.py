#!/usr/bin/env python3
"""Package only a verified five-file pilot for offline team handoff; no credentials or lake data."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "notebooks"))
from pilot_bundle import load_bundle


def pack(bundle, output):
    bundle, output = Path(bundle), Path(output)
    verified = load_bundle(bundle)
    memory = io.BytesIO()
    with zipfile.ZipFile(memory, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(bundle.iterdir()):
            info = zipfile.ZipInfo(path.name, date_time=(2000, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED; info.external_attr = 0o600 << 16
            archive.writestr(info, path.read_bytes())
    data = memory.getvalue()
    if output.is_symlink(): raise ValueError("PILOT_ARCHIVE_SYMLINK_REJECTED")
    if output.exists():
        if output.read_bytes() != data: raise ValueError("PILOT_ARCHIVE_CONFLICT_NO_OVERWRITE")
    else:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as file: file.write(data)
    return {"pilot_id": verified["summary"]["pilot_id"], "files": 5, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("bundle", type=Path); parser.add_argument("output", type=Path)
    args = parser.parse_args(); print(json.dumps(pack(args.bundle, args.output), sort_keys=True))
