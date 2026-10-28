#!/usr/bin/env python3
"""Offline assignment/input readiness only. Never executes ETL, Docker or Drive mutations."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config/research/team_assignments_v1.json"
OWNERS = ("HoaiTam", "ThanhTris", "rosy179")


def require(value, reason):
    if not value:
        raise ValueError("DAT03_SETUP_" + reason)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def months(start, end):
    require(all(isinstance(s, str) and re.fullmatch(r"20\d{2}-(0[1-9]|1[0-2])", s)
                for s in (start, end)), "MONTH_INVALID")
    def number(s):
        y, m = map(int, s.split("-")); return y * 12 + m - 1
    a, b = number(start), number(end)
    require(0 < b - a <= 288, "RANGE_INVALID")
    return [f"{n // 12:04d}-{n % 12 + 1:02d}" for n in range(a, b)]


def load_config():
    config = json.loads(CONFIG.read_text())
    require(config["version"] == "dat03-team-setup-v1"
            and config["execution_enabled"] is False and config["research_ready"] is False,
            "SETUP_ONLY")
    require(set(config["assignments"]) == set(OWNERS), "EXACT_OWNERS")
    covered = []
    for owner in OWNERS:
        entry = config["assignments"][owner]
        interval = months(entry["start"], entry["end_exclusive"])
        require(len(interval) == entry["months"], "MONTH_COUNT")
        covered.extend(interval)
    require(covered == months("2008-06", "2024-01"), "GAP_OR_OVERLAP")
    for name, pin in config["config_sha256"].items():
        require(name in {"config/jma/hypocenter_archives_v1.csv",
                         "config/jma/research_boundary_archives_v1.csv"}, "CONFIG_PATH")
        require(sha(ROOT / name) == pin, "CONFIG_PIN_CHANGED")
    return config


def plan(owner):
    config = load_config()
    require(owner in OWNERS, "OWNER_INVALID")
    entry = config["assignments"][owner]
    interval = months(entry["start"], entry["end_exclusive"])
    accepted = entry["accepted_through"]
    remaining = [m for m in interval if accepted is None or m > accepted]
    return {"status": "ASSIGNMENT_PINNED_NOT_EXECUTABLE", "owner": owner,
            "assignment": entry, "remaining_months": remaining,
            "transport_groups_max_24_months": [remaining[i:i+24] for i in range(0, len(remaining), 24)],
            "config_sha256": sha(CONFIG), "source_remote": config["source_remote"],
            "source_manifest_sha256": config["source_manifest_sha256"],
            "source_cutoff_utc": config["source_cutoff_utc"],
            "namespace_requirement": "new local namespace and own lease; never copy HoaiTam namespace",
            "runtime": "NOT_CHECKED", "drive_identity": "NOT_CHECKED",
            "remaining_gates": config["remaining_gates"],
            "execution_enabled": False, "research_ready": False}


def check_code(expected, root=ROOT):
    require(bool(re.fullmatch(r"[a-f0-9]{40}", expected)), "FULL_COMMIT_REQUIRED")
    def git(*args):
        return subprocess.run(["git", "-C", str(root), *args], check=True,
                              capture_output=True, text=True, timeout=20).stdout.strip()
    require(git("rev-parse", "HEAD") == expected, "WRONG_CODE_RELEASE")
    require(not git("status", "--porcelain", "--untracked-files=normal"), "DIRTY_CHECKOUT")
    return expected


def verify_inputs(owner, source_root, expected_commit):
    check_code(expected_commit)
    report = plan(owner); config = load_config()
    # Trusted repository verifier, not arbitrary code from the downloaded folder.
    spec = importlib.util.spec_from_file_location("jma_handoff", ROOT / "scripts/verify-jma-source-handoff.py")
    verifier = importlib.util.module_from_spec(spec); spec.loader.exec_module(verifier)
    result = verifier.verify(source_root, config["source_manifest_sha256"])
    require(sha(Path(source_root) / "verify_sources.py") == config["source_verifier_sha256"], "VERIFIER_PIN_CHANGED")
    return {**report, "status": "JMA_INPUT_BYTES_VERIFIED_NOT_RANGE_READY",
            "code_commit": expected_commit, "source_verification": result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "verify-inputs"))
    parser.add_argument("--owner", choices=OWNERS, required=True)
    parser.add_argument("--source-root", type=Path)
    parser.add_argument("--approved-commit")
    args = parser.parse_args()
    if args.mode == "verify-inputs":
        if not args.source_root or not args.approved_commit:
            parser.error("--source-root and --approved-commit required")
        result = verify_inputs(args.owner, args.source_root, args.approved_commit)
    else:
        result = plan(args.owner)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        # Do not emit raw paths, command output or credential-bearing diagnostics.
        reason = str(error) if isinstance(error, ValueError) and str(error).startswith("DAT03_SETUP_") else "DAT03_SETUP_CHECK_FAILED"
        print(json.dumps({"status": "BLOCKED", "reason": reason}), file=sys.stderr)
        sys.exit(1)
