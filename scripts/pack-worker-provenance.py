#!/usr/bin/env python3
"""DAT-03 worker provenance packaging and verification tool.

Packages and verifies first-hop provenance metadata objects (Bronze manifests,
Silver bundle manifests, snapshot registry JSONs) for worker nodes
(ThanhTris: 75 objects, rosy179: 83 objects).

Fail-Closed Policy:
- Missing required manifests or registry records fail verification immediately.
- Tampered or empty files are strictly rejected.
- SHA-256 digests are verified for every single object in the archive.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
import zipfile

ROOT = Path(__file__).resolve().parents[1]

# Specifications for required first-hop provenance objects per worker
WORKER_PROVENANCE_SPECS: Dict[str, Dict[str, Any]] = {
    "ThanhTris": {
        "range": ("2013-08", "2018-09"),
        "month_count": 62,
        "total_required_objects": 75,
        "bronze_manifest_count": 37,
        "silver_manifest_count": 19,
        "registry_json_count": 19,
    },
    "rosy179": {
        "range": ("2018-10", "2023-12"),
        "month_count": 63,
        "total_required_objects": 83,
        "bronze_manifest_count": 41,
        "silver_manifest_count": 21,
        "registry_json_count": 21,
    },
}


def compute_sha256(data: bytes) -> str:
    """Compute hex SHA-256 digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def compute_file_sha256(file_path: Path) -> str:
    """Compute hex SHA-256 digest of a file in chunks."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def generate_expected_provenance_manifest(owner: str) -> Dict[str, Any]:
    """Generate the template manifest of required objects for a given worker owner."""
    if owner not in WORKER_PROVENANCE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}. Must be one of {list(WORKER_PROVENANCE_SPECS.keys())}")

    spec = WORKER_PROVENANCE_SPECS[owner]
    start_m, end_m = spec["range"]

    objects: List[Dict[str, str]] = []

    # Bronze manifests
    for i in range(1, spec["bronze_manifest_count"] + 1):
        objects.append({
            "category": "bronze_manifest",
            "path": f"bronze/manifest_{i:02d}.json",
            "required": True,
        })

    # Silver bundle manifests
    for i in range(1, spec["silver_manifest_count"] + 1):
        objects.append({
            "category": "silver_bundle_manifest",
            "path": f"silver/bundle_manifest_{i:02d}.json",
            "required": True,
        })

    # Registry JSONs
    for i in range(1, spec["registry_json_count"] + 1):
        objects.append({
            "category": "registry_json",
            "path": f"registry/snapshot_registry_{i:02d}.json",
            "required": True,
        })

    return {
        "version": "dat03-provenance-spec-v1",
        "owner": owner,
        "range": f"{start_m}..{end_m}",
        "month_count": spec["month_count"],
        "total_required_objects": spec["total_required_objects"],
        "counts_by_category": {
            "bronze_manifest": spec["bronze_manifest_count"],
            "silver_bundle_manifest": spec["silver_manifest_count"],
            "registry_json": spec["registry_json_count"],
        },
        "required_objects": objects,
    }


def pack_provenance_archive(
    owner: str,
    source_dir: Path,
    output_zip: Path,
    expected_spec_file: Optional[Path] = None,
) -> Dict[str, Any]:
    """Pack provenance files from source_dir into a zip archive with fail-closed validation."""
    if owner not in WORKER_PROVENANCE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}")

    source_dir = Path(source_dir).resolve()
    if not source_dir.is_dir():
        raise FileNotFoundError(f"SOURCE_DIR_NOT_FOUND: {source_dir}")

    spec = WORKER_PROVENANCE_SPECS[owner]
    required_count = spec["total_required_objects"]

    # If an expected manifest specification file is provided, load it; otherwise generate standard spec
    if expected_spec_file and Path(expected_spec_file).is_file():
        with open(expected_spec_file, "r", encoding="utf-8") as f:
            template = json.load(f)
    else:
        template = generate_expected_provenance_manifest(owner)

    expected_objects = template.get("required_objects", [])
    if len(expected_objects) != required_count:
        raise ValueError(
            f"INVALID_SPEC_COUNT: expected {required_count} objects for {owner}, got {len(expected_objects)}"
        )

    missing_files: List[str] = []
    empty_files: List[str] = []
    packaged_items: List[Dict[str, Any]] = []

    for item in expected_objects:
        rel_path = item["path"]
        fpath = source_dir / rel_path
        if not fpath.is_file():
            missing_files.append(rel_path)
            continue
        st = fpath.stat()
        if st.st_size == 0:
            empty_files.append(rel_path)
            continue

        sha = compute_file_sha256(fpath)
        packaged_items.append({
            "category": item.get("category", "unknown"),
            "path": rel_path,
            "bytes": st.st_size,
            "sha256": sha,
        })

    if missing_files or empty_files:
        return {
            "status": "FAIL",
            "owner": owner,
            "error": "INCOMPLETE_PROVENANCE_OBJECTS",
            "missing_count": len(missing_files),
            "missing_files": missing_files,
            "empty_count": len(empty_files),
            "empty_files": empty_files,
            "packaged_count": len(packaged_items),
            "required_count": required_count,
        }

    output_zip = Path(output_zip).resolve()
    output_zip.parent.mkdir(parents=True, exist_ok=True)

    manifest_data = {
        "version": "dat03-provenance-package-v1",
        "owner": owner,
        "range": template["range"],
        "month_count": template["month_count"],
        "total_objects": len(packaged_items),
        "required_objects": required_count,
        "packaged_at_utc": datetime.now(timezone.utc).isoformat(),
        "objects": packaged_items,
    }
    manifest_bytes = json.dumps(manifest_data, indent=2).encode("utf-8")
    manifest_sha = compute_sha256(manifest_bytes)

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        # Add manifest as first entry
        zf.writestr("manifest.json", manifest_bytes)
        for item in packaged_items:
            fpath = source_dir / item["path"]
            zf.write(fpath, arcname=item["path"])

    zip_sha = compute_file_sha256(output_zip)

    return {
        "status": "PASS",
        "owner": owner,
        "output_zip": str(output_zip),
        "zip_sha256": zip_sha,
        "manifest_sha256": manifest_sha,
        "total_objects": len(packaged_items),
        "required_count": required_count,
    }


def verify_provenance_archive(
    owner: str,
    zip_path: Path,
    expected_checksums: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Verify that a packaged provenance zip matches all contract and hash constraints."""
    if owner not in WORKER_PROVENANCE_SPECS:
        return {
            "status": "FAIL",
            "reason": f"UNRECOGNIZED_OWNER: {owner}",
        }

    zip_path = Path(zip_path).resolve()
    if not zip_path.is_file():
        return {
            "status": "FAIL",
            "reason": f"ZIP_FILE_NOT_FOUND: {zip_path}",
        }

    spec = WORKER_PROVENANCE_SPECS[owner]
    required_count = spec["total_required_objects"]

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            namelist = set(zf.namelist())
            if "manifest.json" not in namelist:
                return {
                    "status": "FAIL",
                    "reason": "MISSING_MANIFEST_JSON_IN_ZIP",
                }

            manifest_content = zf.read("manifest.json")
            manifest = json.loads(manifest_content.decode("utf-8"))

            if manifest.get("owner") != owner:
                return {
                    "status": "FAIL",
                    "reason": f"OWNER_MISMATCH: expected {owner}, manifest has {manifest.get('owner')}",
                }

            objects = manifest.get("objects", [])
            if len(objects) != required_count:
                return {
                    "status": "FAIL",
                    "reason": f"OBJECT_COUNT_MISMATCH: expected {required_count}, got {len(objects)}",
                    "actual_count": len(objects),
                    "expected_count": required_count,
                }

            # Check counts by category
            bronze_cnt = sum(1 for o in objects if o.get("category") == "bronze_manifest")
            silver_cnt = sum(1 for o in objects if o.get("category") == "silver_bundle_manifest")
            reg_cnt = sum(1 for o in objects if o.get("category") == "registry_json")

            if (
                bronze_cnt != spec["bronze_manifest_count"]
                or silver_cnt != spec["silver_manifest_count"]
                or reg_cnt != spec["registry_json_count"]
            ):
                return {
                    "status": "FAIL",
                    "reason": "CATEGORY_BREAKDOWN_MISMATCH",
                    "breakdown": {
                        "bronze_manifest": (bronze_cnt, spec["bronze_manifest_count"]),
                        "silver_bundle_manifest": (silver_cnt, spec["silver_manifest_count"]),
                        "registry_json": (reg_cnt, spec["registry_json_count"]),
                    },
                }

            # Verify integrity of every object inside zip
            hash_mismatches: List[str] = []
            corrupt_files: List[str] = []

            for obj in objects:
                rel_path = obj["path"]
                if rel_path not in namelist:
                    corrupt_files.append(f"FILE_MISSING_IN_ZIP:{rel_path}")
                    continue

                raw = zf.read(rel_path)
                computed_sha = compute_sha256(raw)
                expected_sha = obj.get("sha256")

                if computed_sha != expected_sha:
                    hash_mismatches.append(
                        f"HASH_MISMATCH:{rel_path}: computed={computed_sha}, manifest={expected_sha}"
                    )

                if expected_checksums and rel_path in expected_checksums:
                    ext_sha = expected_checksums[rel_path]
                    if computed_sha != ext_sha:
                        hash_mismatches.append(
                            f"EXTERNAL_HASH_MISMATCH:{rel_path}: computed={computed_sha}, expected={ext_sha}"
                        )

            if hash_mismatches or corrupt_files:
                return {
                    "status": "FAIL",
                    "reason": "INTEGRITY_VERIFICATION_FAILED",
                    "hash_mismatch_count": len(hash_mismatches),
                    "hash_mismatches": hash_mismatches,
                    "corrupt_files": corrupt_files,
                }

            return {
                "status": "PASS",
                "owner": owner,
                "verified_objects": len(objects),
                "required_objects": required_count,
                "bronze_manifests": bronze_cnt,
                "silver_manifests": silver_cnt,
                "registry_records": reg_cnt,
                "zip_sha256": compute_file_sha256(zip_path),
            }

    except Exception as ex:
        return {
            "status": "FAIL",
            "reason": f"EXCEPTION_DURING_VERIFICATION: {ex}",
        }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="DAT-03 worker provenance packaging and verification tool."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: plan
    p_plan = subparsers.add_parser("plan", help="Output expected provenance object specification.")
    p_plan.add_argument("--owner", required=True, choices=["ThanhTris", "rosy179"])
    p_plan.add_argument("--output-json", help="Path to write required provenance manifest JSON.")

    # Subcommand: pack
    p_pack = subparsers.add_parser("pack", help="Pack provenance objects into zip.")
    p_pack.add_argument("--owner", required=True, choices=["ThanhTris", "rosy179"])
    p_pack.add_argument("--source-dir", required=True, help="Directory containing unpacked provenance files.")
    p_pack.add_argument("--output-zip", required=True, help="Path for output ZIP file.")
    p_pack.add_argument("--spec-file", help="Optional path to expected provenance spec JSON.")

    # Subcommand: verify
    p_verify = subparsers.add_parser("verify", help="Verify provenance zip archive.")
    p_verify.add_argument("--owner", required=True, choices=["ThanhTris", "rosy179"])
    p_verify.add_argument("--zip-file", required=True, help="Path to provenance ZIP file.")
    p_verify.add_argument("--checksums-file", help="Optional JSON file with expected SHA-256 for each object.")

    args = parser.parse_args(argv)

    if args.command == "plan":
        plan_data = generate_expected_provenance_manifest(args.owner)
        if args.output_json:
            out_p = Path(args.output_json).resolve()
            out_p.parent.mkdir(parents=True, exist_ok=True)
            with open(out_p, "w", encoding="utf-8") as f:
                json.dump(plan_data, f, indent=2)
            print(f"Wrote plan to {out_p}")
        else:
            print(json.dumps(plan_data, indent=2))
        return 0

    if args.command == "pack":
        res = pack_provenance_archive(
            owner=args.owner,
            source_dir=Path(args.source_dir),
            output_zip=Path(args.output_zip),
            expected_spec_file=Path(args.spec_file) if args.spec_file else None,
        )
        print(json.dumps(res, indent=2))
        return 0 if res.get("status") == "PASS" else 1

    if args.command == "verify":
        checksums = None
        if args.checksums_file:
            with open(args.checksums_file, "r", encoding="utf-8") as f:
                checksums = json.load(f)
        res = verify_provenance_archive(
            owner=args.owner,
            zip_path=Path(args.zip_file),
            expected_checksums=checksums,
        )
        print(json.dumps(res, indent=2))
        return 0 if res.get("status") == "PASS" else 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
