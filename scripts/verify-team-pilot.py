#!/usr/bin/env python3
"""Offline team-kit verification only; no ETL, credentials, network or official lifecycle writes."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys


ROSTER = ("HoaiTam", "ThanhTris", "rosy179")
FILES = {
    "README.md", "requirements.txt", "verify_dataset.py", "tools/pilot_bundle.py",
    "bundle/features-part-00000.parquet", "bundle/mainshocks.parquet",
    "bundle/pilot_manifest.json", "bundle/coverage_report.json", "bundle/checksums.sha256",
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def verify(root, expected_handoff_sha256, recipient, receipt=None):
    root = Path(root)
    require(recipient in ROSTER, "TEAM_RECIPIENT_REQUIRED")
    require(re.fullmatch(r"[0-9a-f]{64}", expected_handoff_sha256 or ""), "TEAM_EXTERNAL_PIN_REQUIRED")
    require(root.is_dir() and not root.is_symlink(), "TEAM_DIRECTORY_REQUIRED")
    paths = list(root.rglob("*"))
    require(not any(p.is_symlink() for p in paths), "TEAM_SYMLINK_REJECTED")
    actual = {p.relative_to(root).as_posix() for p in paths if p.is_file()}
    require(actual == FILES | {"handoff.json"}, "TEAM_FILE_SET_MISMATCH")
    raw = (root / "handoff.json").read_bytes()
    require(sha(raw) == expected_handoff_sha256, "TEAM_HANDOFF_PIN_MISMATCH")
    handoff = json.loads(raw)
    require(handoff.get("version") == "dat03-team-pilot-v1"
            and handoff.get("artifact_kind") == "DEVELOPMENT_PILOT"
            and handoff.get("allowed_usage") == "DEVELOPMENT_ONLY"
            and handoff.get("research_ready") is False, "TEAM_DEVELOPMENT_ONLY_REQUIRED")
    require(set(handoff.get("files", {})) == FILES, "TEAM_FILE_SET_MISMATCH")
    for name in sorted(FILES):
        entry = handoff["files"][name]
        path = root / name
        require(path.stat().st_size == entry["bytes"] and sha(path.read_bytes()) == entry["sha256"],
                "TEAM_FILE_CHECKSUM_MISMATCH:" + name)
    # Import only after verifying every shipped code/data file against the external pin.
    sys.dont_write_bytecode = True
    import importlib.util
    spec = importlib.util.spec_from_file_location("verified_team_pilot", root / "tools/pilot_bundle.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    summary = module.load_bundle(root / "bundle")["summary"]
    require(summary["pilot_id"] == handoff["pilot_id"]
            and summary["features"] == handoff["feature_count"]
            and summary["mainshocks"] == handoff["mainshock_count"]
            and summary["coverage_status"] == handoff["coverage_status"], "TEAM_COUNTS_MISMATCH")
    result = {
        "version": "dat03-team-receipt-v1", "status": "TEAM_PILOT_VERIFIED",
        "recipient": recipient, "verified_at_utc": datetime.now(timezone.utc).isoformat(),
        "handoff_sha256": expected_handoff_sha256, "pilot_id": summary["pilot_id"],
        "feature_count": summary["features"], "mainshock_count": summary["mainshocks"],
        "coverage_status": summary["coverage_status"], "research_ready": False,
        "allowed_usage": "DEVELOPMENT_ONLY", "recipient_self_reported": True,
        "scientific_approval": False,
    }
    if receipt is not None:
        receipt = Path(receipt)
        require(root.resolve() not in receipt.resolve().parents and receipt.resolve() != root.resolve(),
                "TEAM_RECEIPT_MUST_BE_OUTSIDE_KIT")
        require(not receipt.is_symlink(), "TEAM_RECEIPT_SYMLINK_REJECTED")
        with receipt.open("x", encoding="utf-8") as output:
            json.dump(result, output, indent=2, sort_keys=True)
            output.write("\n")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kit", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--expected-handoff-sha256", required=True)
    parser.add_argument("--recipient", required=True, choices=ROSTER)
    parser.add_argument("--receipt", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.kit, args.expected_handoff_sha256, args.recipient, args.receipt), indent=2))
