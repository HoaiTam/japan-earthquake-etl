#!/usr/bin/env python3
"""DAT-03 worker provenance packaging and verification tool.

Packages and verifies first-hop provenance metadata objects (Bronze manifests,
Silver bundle manifests, snapshot registry JSONs) for worker nodes
(ThanhTris: 75 objects, rosy179: 83 objects).

Fail-Closed Policy:
- Missing required manifests or registry records fail verification immediately.
- Tampered, empty, or duplicate files are strictly rejected.
- Paths with directory traversal (..) or absolute paths are strictly rejected.
- Self-integrity mode validates ZIP consistency against internal manifest.
- Trusted provenance mode validates against external pinned requirement mapping;
  missing keys, count mismatches, or checksum mismatches fail immediately.
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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Union
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
        "bronze_manifest_count": 37,
        "silver_manifest_count": 23,
        "registry_json_count": 23,
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


def is_safe_relpath(path_str: str) -> bool:
    """Validate that path is relative, normalized, and cannot escape root."""
    if not path_str or not isinstance(path_str, str):
        return False
    if "\0" in path_str:
        return False
    norm = path_str.replace("\\", "/")
    if norm.startswith("/") or re.match(r"^[a-zA-Z]:", norm):
        return False
    parts = norm.split("/")
    if ".." in parts or "." in parts or "" in parts:
        return False
    return True


def normalize_relpath(path_str: str) -> str:
    """Normalize path string to forward-slash relative path."""
    clean = str(path_str).replace("\\", "/").strip("/")
    if clean.startswith("s3://japan-earthquake/"):
        clean = clean[len("s3://japan-earthquake/"):]
    return clean


def parse_trusted_checksums(data: Any) -> Dict[str, str]:
    """Parse trusted checksum mapping from various JSON formats into {relpath: sha256}.

    Supported input formats:
    - Path or str pointing to JSON file
    - Dict of {path_or_uri: sha256}
    - Dict with "checksums": {path: sha256}
    - Dict with "objects": [{"path"|"archive_path"|"s3_uri": ..., "sha256": ...}]
    - List of [{"path"|"archive_path"|"s3_uri": ..., "sha256": ...}]
    """
    if isinstance(data, (str, Path)):
        p = Path(data).resolve()
        if p.is_file():
            with open(p, "r", encoding="utf-8") as f:
                data = json.load(f)

    mapping: Dict[str, str] = {}

    if isinstance(data, dict):
        if "checksums" in data and isinstance(data["checksums"], dict):
            raw_map = data["checksums"]
            for k, v in raw_map.items():
                norm = normalize_relpath(k)
                mapping[norm] = str(v).strip().lower()
            return mapping

        if "objects" in data and isinstance(data["objects"], list):
            data = data["objects"]
        elif "required_objects" in data and isinstance(data["required_objects"], list):
            data = data["required_objects"]
        else:
            # Assume flat {key: sha}
            for k, v in data.items():
                if isinstance(v, str) and re.fullmatch(r"[0-9a-fA-F]{64}", v.strip()):
                    norm = normalize_relpath(k)
                    mapping[norm] = v.strip().lower()
            if mapping:
                return mapping

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            p = item.get("path") or item.get("archive_path") or item.get("s3_uri")
            sha = item.get("sha256")
            if p and sha:
                norm = normalize_relpath(p)
                mapping[norm] = str(sha).strip().lower()

    return mapping


def generate_expected_provenance_manifest(owner: str) -> Dict[str, Any]:
    """Generate the template manifest of required objects for a given worker owner."""
    if owner not in WORKER_PROVENANCE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}. Must be one of {list(WORKER_PROVENANCE_SPECS.keys())}")

    spec = WORKER_PROVENANCE_SPECS[owner]
    start_m, end_m = spec["range"]

    objects: List[Dict[str, Any]] = []

    # Bronze manifests
    for i in range(1, spec["bronze_manifest_count"] + 1):
        rel = f"bronze/manifest_{i:02d}.json"
        objects.append({
            "category": "bronze_manifest",
            "path": rel,
            "archive_path": rel,
            "required": True,
        })

    # Silver bundle manifests
    for i in range(1, spec["silver_manifest_count"] + 1):
        rel = f"silver/bundle_manifest_{i:02d}.json"
        objects.append({
            "category": "silver_bundle_manifest",
            "path": rel,
            "archive_path": rel,
            "required": True,
        })

    # Registry JSONs
    for i in range(1, spec["registry_json_count"] + 1):
        rel = f"registry/snapshot_registry_{i:02d}.json"
        objects.append({
            "category": "registry_json",
            "path": rel,
            "archive_path": rel,
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
    overwrite: bool = True,
) -> Dict[str, Any]:
    """Pack provenance files from source_dir into a zip archive with fail-closed validation."""
    if owner not in WORKER_PROVENANCE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}")

    source_dir = Path(source_dir).resolve()
    if not source_dir.is_dir():
        raise FileNotFoundError(f"SOURCE_DIR_NOT_FOUND: {source_dir}")

    output_zip = Path(output_zip).resolve()
    if output_zip.exists() and not overwrite:
        raise FileExistsError(f"OUTPUT_FILE_ALREADY_EXISTS: {output_zip}")

    spec = WORKER_PROVENANCE_SPECS[owner]
    required_count = spec["total_required_objects"]

    if expected_spec_file and Path(expected_spec_file).is_file():
        with open(expected_spec_file, "r", encoding="utf-8") as f:
            template = json.load(f)
    else:
        template = generate_expected_provenance_manifest(owner)

    raw_objects = template.get("required_objects") or template.get("objects", [])
    if len(raw_objects) != required_count:
        raise ValueError(
            f"INVALID_SPEC_COUNT: expected {required_count} objects for {owner}, got {len(raw_objects)}"
        )

    # Validate uniqueness and safety of expected paths
    seen_paths: Set[str] = set()
    expected_objects: List[Dict[str, Any]] = []
    for item in raw_objects:
        rel_path = normalize_relpath(item.get("path") or item.get("archive_path", ""))
        if not is_safe_relpath(rel_path):
            raise ValueError(f"UNSAFE_OBJECT_PATH: {rel_path}")
        if rel_path in seen_paths:
            raise ValueError(f"DUPLICATE_OBJECT_PATH_IN_SPEC: {rel_path}")
        seen_paths.add(rel_path)
        expected_objects.append({
            "category": item.get("category", "unknown"),
            "path": rel_path,
            "archive_path": rel_path,
            "s3_uri": item.get("s3_uri"),
        })

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
            "archive_path": rel_path,
            "s3_uri": item.get("s3_uri") or f"s3://japan-earthquake/{rel_path}",
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

    output_zip.parent.mkdir(parents=True, exist_ok=True)

    manifest_data = {
        "version": "dat03-provenance-package-v1",
        "owner": owner,
        "range": template.get("range", f"{spec['range'][0]}..{spec['range'][1]}"),
        "month_count": template.get("month_count", spec["month_count"]),
        "total_objects": len(packaged_items),
        "required_objects": required_count,
        "counts_by_category": {
            "bronze_manifest": sum(1 for x in packaged_items if x["category"] == "bronze_manifest"),
            "silver_bundle_manifest": sum(1 for x in packaged_items if x["category"] == "silver_bundle_manifest"),
            "registry_json": sum(1 for x in packaged_items if x["category"] == "registry_json"),
        },
        "packaged_at_utc": datetime.now(timezone.utc).isoformat(),
        "objects": packaged_items,
    }
    manifest_bytes = json.dumps(manifest_data, indent=2).encode("utf-8")
    manifest_sha = compute_sha256(manifest_bytes)

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
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
        "counts_by_category": manifest_data["counts_by_category"],
    }


def verify_provenance_archive(
    owner: str,
    zip_path: Path,
    expected_checksums: Optional[Union[Dict[str, Any], Sequence[Any]]] = None,
    require_trusted: bool = False,
) -> Dict[str, Any]:
    """Verify that a packaged provenance zip matches all contract, safety, and hash constraints.

    Verification levels:
    - self_integrity: validates zip structure, unique member paths, non-empty files,
      and object hashes against zip internal manifest.
    - trusted_provenance: in addition to self-integrity, validates all required objects
      against an external pinned trusted checksum mapping (fail-closed if any required
      key is missing or hash mismatches).
    """
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

            # 1. Enforce strict uniqueness and safety of object paths
            seen_paths: Set[str] = set()
            duplicate_paths: List[str] = []
            unsafe_paths: List[str] = []
            normalized_objects: List[Dict[str, Any]] = []

            for obj in objects:
                raw_path = obj.get("path") or obj.get("archive_path")
                if not raw_path:
                    return {
                        "status": "FAIL",
                        "reason": "OBJECT_MISSING_PATH_IN_MANIFEST",
                        "sample_object": obj,
                    }
                norm_p = normalize_relpath(str(raw_path))
                if not is_safe_relpath(norm_p):
                    unsafe_paths.append(str(raw_path))
                    continue
                if norm_p in seen_paths:
                    duplicate_paths.append(norm_p)
                seen_paths.add(norm_p)

                normalized_objects.append({
                    "category": obj.get("category"),
                    "path": norm_p,
                    "archive_path": norm_p,
                    "s3_uri": obj.get("s3_uri"),
                    "bytes": obj.get("bytes"),
                    "sha256": str(obj.get("sha256", "")).strip().lower(),
                })

            if unsafe_paths:
                return {
                    "status": "FAIL",
                    "reason": "UNSAFE_OBJECT_PATH",
                    "unsafe_paths": unsafe_paths,
                }

            if duplicate_paths:
                return {
                    "status": "FAIL",
                    "reason": "DUPLICATE_OBJECT_PATH",
                    "duplicate_count": len(duplicate_paths),
                    "duplicate_paths": duplicate_paths,
                }

            if len(seen_paths) != required_count:
                return {
                    "status": "FAIL",
                    "reason": f"UNIQUE_PATH_COUNT_MISMATCH: expected {required_count} unique paths, got {len(seen_paths)}",
                }

            # 2. Check counts by category
            bronze_cnt = sum(1 for o in normalized_objects if o.get("category") == "bronze_manifest")
            silver_cnt = sum(1 for o in normalized_objects if o.get("category") == "silver_bundle_manifest")
            reg_cnt = sum(1 for o in normalized_objects if o.get("category") == "registry_json")

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

            # 3. Verify member existence and self-integrity SHA-256 for each object
            hash_mismatches: List[str] = []
            missing_members: List[str] = []
            empty_members: List[str] = []

            actual_hashes: Dict[str, str] = {}

            for obj in normalized_objects:
                rel_path = obj["path"]
                if rel_path not in namelist:
                    missing_members.append(rel_path)
                    continue

                raw = zf.read(rel_path)
                if len(raw) == 0:
                    empty_members.append(rel_path)
                    continue

                computed_sha = compute_sha256(raw)
                actual_hashes[rel_path] = computed_sha
                expected_sha = obj.get("sha256")

                if computed_sha != expected_sha:
                    hash_mismatches.append(
                        f"SELF_HASH_MISMATCH:{rel_path}: computed={computed_sha}, manifest={expected_sha}"
                    )

            if missing_members or empty_members or hash_mismatches:
                return {
                    "status": "FAIL",
                    "reason": "SELF_INTEGRITY_VERIFICATION_FAILED",
                    "missing_members": missing_members,
                    "empty_members": empty_members,
                    "hash_mismatches": hash_mismatches,
                }

            # 4. Trusted Provenance Verification against external trusted mapping
            trusted_verified = False
            trusted_objects_verified = 0

            if expected_checksums is not None or require_trusted:
                if expected_checksums is None:
                    return {
                        "status": "FAIL",
                        "reason": "REQUIRED_TRUSTED_CHECKSUMS_MISSING: Verification required trusted mapping but none provided.",
                    }

                trusted_map = parse_trusted_checksums(expected_checksums)
                if not trusted_map:
                    return {
                        "status": "FAIL",
                        "reason": "EMPTY_TRUSTED_CHECKSUMS_MAP: Pinned requirements map is empty.",
                    }

                missing_trusted_keys: List[str] = []
                trusted_hash_mismatches: List[str] = []

                for obj in normalized_objects:
                    rel_path = obj["path"]
                    if rel_path not in trusted_map:
                        missing_trusted_keys.append(rel_path)
                        continue

                    trusted_sha = trusted_map[rel_path]
                    computed_sha = actual_hashes[rel_path]
                    if computed_sha != trusted_sha:
                        trusted_hash_mismatches.append(
                            f"TRUSTED_HASH_MISMATCH:{rel_path}: computed={computed_sha}, trusted={trusted_sha}"
                        )
                    else:
                        trusted_objects_verified += 1

                if missing_trusted_keys:
                    return {
                        "status": "FAIL",
                        "reason": "INCOMPLETE_TRUSTED_CHECKSUMS",
                        "missing_count": len(missing_trusted_keys),
                        "missing_keys": missing_trusted_keys,
                    }

                if trusted_hash_mismatches:
                    return {
                        "status": "FAIL",
                        "reason": "TRUSTED_PROVENANCE_HASH_MISMATCH",
                        "mismatch_count": len(trusted_hash_mismatches),
                        "mismatches": trusted_hash_mismatches,
                    }

                trusted_verified = True

            zip_sha = compute_file_sha256(zip_path)

            return {
                "status": "PASS",
                "owner": owner,
                "verification_level": "trusted_provenance" if trusted_verified else "self_integrity_only",
                "verified_objects": len(normalized_objects),
                "required_objects": required_count,
                "trusted_objects_verified": trusted_objects_verified if trusted_verified else 0,
                "bronze_manifests": bronze_cnt,
                "silver_manifests": silver_cnt,
                "registry_records": reg_cnt,
                "zip_sha256": zip_sha,
                "manifest_sha256": compute_sha256(manifest_content),
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
    p_pack.add_argument("--overwrite", action="store_true", default=False, help="Overwrite output ZIP if exists.")

    # Subcommand: verify
    p_verify = subparsers.add_parser("verify", help="Verify provenance zip archive.")
    p_verify.add_argument("--owner", required=True, choices=["ThanhTris", "rosy179"])
    p_verify.add_argument("--zip-file", required=True, help="Path to provenance ZIP file.")
    p_verify.add_argument("--checksums-file", help="Optional JSON file with expected SHA-256 for each object.")
    p_verify.add_argument("--require-trusted", action="store_true", default=False, help="Fail if trusted checksums file is not provided.")

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
        try:
            res = pack_provenance_archive(
                owner=args.owner,
                source_dir=Path(args.source_dir),
                output_zip=Path(args.output_zip),
                expected_spec_file=Path(args.spec_file) if args.spec_file else None,
                overwrite=args.overwrite,
            )
            print(json.dumps(res, indent=2))
            return 0 if res.get("status") == "PASS" else 1
        except Exception as ex:
            print(json.dumps({"status": "FAIL", "reason": str(ex)}, indent=2))
            return 1

    if args.command == "verify":
        checksums = None
        if args.checksums_file:
            cpath = Path(args.checksums_file).resolve()
            if not cpath.is_file():
                print(json.dumps({"status": "FAIL", "reason": f"CHECKSUMS_FILE_NOT_FOUND: {cpath}"}, indent=2))
                return 1
            with open(cpath, "r", encoding="utf-8") as f:
                checksums = json.load(f)

        res = verify_provenance_archive(
            owner=args.owner,
            zip_path=Path(args.zip_file),
            expected_checksums=checksums,
            require_trusted=args.require_trusted,
        )
        print(json.dumps(res, indent=2))
        return 0 if res.get("status") == "PASS" else 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
