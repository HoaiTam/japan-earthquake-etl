#!/usr/bin/env python3
"""Read-only copy of exact, previously accepted MLD-03 artifacts. No ETL/build/start/init."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def stage(evidence_path, destination, split):
    evidence_path, destination = Path(evidence_path), Path(destination)
    if evidence_path.stat().st_size > 2_097_152 or destination.is_symlink():
        raise ValueError("ML_WINDOW_STAGE_BOUNDED_PATH_REQUIRED")
    evidence = json.loads(evidence_path.read_text())
    source = evidence.get("native_report_path", "")
    if not re.fullmatch(r"/opt/pipeline/staging/mainshock-window-qa/mld03-qa-[a-f0-9]{32}/(?:resume-[a-f0-9]{32}/)?mld-03-report.json", source):
        raise ValueError("ML_WINDOW_STAGE_EXACT_RECEIPT_REQUIRED")
    compose = ["docker", "compose", "--env-file", str(ROOT / ".env"), "-f", str(ROOT / "compose.yaml")]
    def read(path, limit=2_097_152):
        # The helper reads only this explicit path, returns bytes privately, never logs env/errors.
        code = "from pathlib import Path;import sys;p=Path(sys.argv[1]);n=int(sys.argv[2]);assert p.is_file() and not p.is_symlink() and p.stat().st_size<=n;sys.stdout.buffer.write(p.read_bytes())"
        p = subprocess.run(compose + ["exec", "-T", "airflow-api-server", "python", "-c", code, str(path), str(limit)],
                           capture_output=True, timeout=60)
        if p.returncode or len(p.stdout) > limit:
            raise RuntimeError("ML_WINDOW_STAGE_READ_FAILED_REDACTED")
        return p.stdout
    raw = read(source)
    if sha(raw) != evidence.get("native_report_sha256"):
        raise ValueError("ML_WINDOW_STAGE_RECEIPT_CHANGED")
    native = json.loads(raw)
    if native.get("status") != "VERIFIED_PUBLISHED_JMA_WINDOWS" or native.get("research_ready") is not False:
        raise ValueError("ML_WINDOW_STAGE_ACCEPTED_RECEIPT_REQUIRED")
    items = [d for d in native["datasets"] if d["manifest"]["dataset_split"] == split]
    if len(items) != 1:
        raise ValueError("ML_WINDOW_STAGE_EXACT_SPLIT_REQUIRED")
    item = items[0]
    base = PurePosixPath(source).parent
    prefix = split.lower()
    artifacts = {"native-report.json": raw}
    for name, expected in [("window-runtime-report.json", item["window"]), ("window-manifest.json", item["window_manifest"])]:
        data = read(base / prefix / name)
        if json.loads(data) != expected:
            raise ValueError("ML_WINDOW_STAGE_HANDOFF_CONFLICT")
        artifacts[prefix + "/" + name] = data
    entries = item["window"]["parquet_files"]
    if not entries or len(entries) > 1024:
        raise ValueError("ML_WINDOW_STAGE_FILE_LIMIT")
    for entry in entries:
        path = PurePosixPath(entry["path"])
        if path.is_absolute() or ".." in path.parts or not re.fullmatch(
                r"windows/(?:mainshock_candidate_snapshot|sequence_candidate_snapshot)\.parquet/part-[A-Za-z0-9_.-]+\.parquet", str(path)):
            raise ValueError("ML_WINDOW_STAGE_UNSAFE_PART")
        data = read(base / prefix / path, 16_777_216)
        if len(data) != entry["bytes"] or sha(data) != entry["sha256"]:
            raise ValueError("ML_WINDOW_STAGE_PARQUET_CHANGED")
        artifacts[prefix + "/" + str(path)] = data
    receipt = {"input_version": "mld04-window-input-v1", "split": split, "native_report_sha256": sha(raw),
               "files": [{"path": name, "bytes": len(data), "sha256": sha(data)} for name, data in sorted(artifacts.items())]}
    artifacts["source-input.json"] = (json.dumps(receipt, sort_keys=True, indent=2) + "\n").encode()
    destination.parent.mkdir(parents=True, exist_ok=True)
    reused = destination.exists()
    if reused:
        actual = {str(p.relative_to(destination)) for p in destination.rglob("*") if p.is_file()}
        if actual != set(artifacts) or any((destination / name).is_symlink() or (destination / name).read_bytes() != data for name, data in artifacts.items()):
            raise ValueError("ML_WINDOW_STAGE_EXISTING_INPUT_CONFLICT")
    else:
        with tempfile.TemporaryDirectory(prefix="window-stage-", dir=destination.parent) as temporary:
            temp = Path(temporary) / "input"; temp.mkdir()
            for name, data in artifacts.items():
                target = temp / name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(data)
            temp.rename(destination)
    return {"status": "STAGED_VERIFIED_WINDOWS", "split": split, "files": len(artifacts),
            "bytes": sum(map(len, artifacts.values())), "native_report_sha256": sha(raw), "reused": reused}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, default=ROOT / "docs/evidence/MLD-03-window-pilot.json")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--split", choices=["REPRODUCTION", "EXTENSION"], default="REPRODUCTION")
    args = parser.parse_args()
    try:
        print(json.dumps(stage(args.evidence, args.output, args.split), sort_keys=True))
    except Exception:
        raise SystemExit("ML_WINDOW_STAGE_FAILED_REDACTED; inspect exact receipt/path/checksum, no cleanup or restart performed") from None
