#!/usr/bin/env python3
"""Offline notebook handoff verification. Requires only the EXP-01/PyArrow environment, no credentials."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys


def verify(root, manifest_sha, index_sha, loader=None):
    root = Path(root)
    for pin in (manifest_sha, index_sha):
        if not re.fullmatch(r"[a-f0-9]{64}", pin or ""):
            raise ValueError("MLD05_EXPECTED_CHECKSUM_PINS_REQUIRED")
    if root.is_symlink() or not root.is_dir():
        raise ValueError("MLD05_EXACT_BUNDLE_DIRECTORY_REQUIRED")
    for name, expected in (("dataset_manifest.json", manifest_sha), ("checksums.sha256", index_sha)):
        file = root / name
        if file.is_symlink() or not file.is_file() or file.stat().st_size > 32 * 1024 * 1024:
            raise ValueError("MLD05_BOUNDED_ARTIFACT_REQUIRED")
        if hashlib.sha256(file.read_bytes()).hexdigest() != expected:
            raise ValueError("MLD05_PINNED_CHECKSUM_MISMATCH")
    if loader is None:
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "notebooks"))
        from experiment_harness import load_feature_bundle
        loader = load_feature_bundle
    bundle = loader(root, mode="OFFICIAL")
    return {"status": "VERIFIED_EXPORTED_BUNDLE", "dataset_id": bundle["source_dataset_id"],
            "feature_row_count": bundle["features_table"].num_rows, "credentials_required": False,
            "scientific_approval": False, "manifest_sha256": manifest_sha, "checksums_sha256": index_sha}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("bundle", type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--checksums-sha256", required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.bundle, args.manifest_sha256, args.checksums_sha256), sort_keys=True))
    except Exception:
        raise SystemExit("MLD05_OFFLINE_BUNDLE_VERIFICATION_FAILED") from None
