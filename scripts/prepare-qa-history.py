#!/usr/bin/env python3
"""Explicit bounded public captures for QA-03; no parsing/filtering/business ETL."""
import csv
import hashlib
import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("full_month_capture", ROOT / "scripts/full-month-state.py")
capture_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture_module)
NAMES = {"h199701.zip", "h199710.zip", "h2000.zip", "h2023.zip"}


def prepare():
    folder = ROOT / "staging/qa-history-captures"
    if any(parent.is_symlink() for parent in (folder, *folder.parents)):
        raise ValueError("UNSAFE_QA_CAPTURE_ROOT")
    folder.mkdir(parents=True, exist_ok=True)
    with (ROOT / "config/jma/hypocenter_archives_v1.csv").open(newline="") as stream:
        selected = [row for row in csv.DictReader(stream) if row["archive_name"] in NAMES]
    if len(selected) != len(NAMES):
        raise ValueError("QA_HISTORY_INVENTORY_INCOMPLETE")
    for row in selected:
        capture_module.capture(row["source_url"], folder / row["archive_name"])
    capture_module.capture(capture_module.USGS_URL, folder / "usgs-2023-09.geojson")
    for name in NAMES | {"usgs-2023-09.geojson"}:
        file = folder / name
        if file.is_symlink() or not file.is_file() or not 0 < file.stat().st_size <= 32 * 1024 * 1024:
            raise ValueError("QA_HISTORY_CAPTURE_NOT_BOUNDED")
    # Capture reuse is pinned. An existing receipt is never silently refreshed.
    receipt = {name: hashlib.sha256((folder / name).read_bytes()).hexdigest()
               for name in sorted(NAMES | {"usgs-2023-09.geojson"})}
    target = folder / "capture-pins.json"
    if target.exists():
        if target.is_symlink() or json.loads(target.read_text()) != receipt:
            raise ValueError("QA_HISTORY_CAPTURE_PIN_CHANGED")
    else:
        with target.open("x") as stream:
            json.dump(receipt, stream, sort_keys=True, indent=2)
    print("event=qa_history_captures_ready files=5 no_ETL_no_Docker=true")


if __name__ == "__main__":
    prepare()
