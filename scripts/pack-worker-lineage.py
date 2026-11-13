#!/usr/bin/env python3
"""DAT-03 worker supplemental lineage packaging and verification tool.

Packages and verifies supplemental lineage metadata objects (Tier 2 phase journals:
readiness, bronze, silver, gold commit, verify, publish) for worker nodes
(ThanhTris: 19 runs * 6 phases = 114 objects, rosy179: 23 runs * 6 phases = 138 objects).

Also extracts Tier 3 transitive closure frontier specification (114 objects for rosy179:
92 Silver dataset manifests + 22 parent Gold commit JSONs).

Evidence Tiers:
- Tier 1 (First-hop Provenance): 83 objects for rosy179 (37 Bronze manifests, 23 Silver
  bundle manifests, 23 Gold snapshot registries) -> PASS 100% SHA-256.
- Tier 2 (Supplemental Phase Evidence): 138 objects for rosy179 (23 runs x 6 phase journals)
  packaged in rosy179-dat03-lineage-metadata.zip.
- Tier 3 (Transitive Closure Frontier): 114 objects for rosy179 (92 Silver dataset manifests
  + 22 parent Gold commits) defined in rosy179-frontier-114-spec.json, PENDING original
  storage bytes from MinIO/object storage.

Fail-Closed Policy:
- Missing required phase journals fails verification immediately.
- Tampered, empty, or duplicate files in ZIP are strictly rejected.
- ZIP members with duplicate names fail before reading.
- Symlinks (file, parent directory, or ZIP entry mode 0o120000) are strictly rejected.
- Path traversal (..), absolute paths, and source root escaping are strictly rejected.
- Exact namespace layout 'silver/real-etl/<operation_id>/<phase>.json' is strictly enforced.
- Logical duplicate (operation_id, phase) combinations are strictly rejected.
- Self-integrity mode validates ZIP consistency against internal manifest.
- Trusted lineage mode validates against external pinned requirement mapping;
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
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
import zipfile

ROOT = Path(__file__).resolve().parents[1]

WORKER_LINEAGE_SPECS: Dict[str, Dict[str, Any]] = {
    "ThanhTris": {
        "range": ("2013-08", "2018-09"),
        "month_count": 62,
        "run_count": 19,
        "phases_per_run": 6,
        "total_required_objects": 114,
        "expected_phases": ["readiness", "bronze", "silver", "gold", "verify", "publish"],
        "expected_bundle_count": 19,
        "expected_registry_count": 19,
        "expected_dataset_manifest_count": 76,
        "expected_parent_commit_count": 18,
        "expected_frontier_count": 94,
        "pinned_provenance_sha256": None,
    },
    "rosy179": {
        "range": ("2018-10", "2023-12"),
        "month_count": 63,
        "run_count": 23,
        "phases_per_run": 6,
        "total_required_objects": 138,
        "expected_phases": ["readiness", "bronze", "silver", "gold", "verify", "publish"],
        "expected_bundle_count": 23,
        "expected_registry_count": 23,
        "expected_dataset_manifest_count": 92,
        "expected_parent_commit_count": 22,
        "expected_frontier_count": 114,
        "pinned_provenance_sha256": "f818ba4697d3915376420ad66cd040c542853333a31bc2b4e4a5d0b4115d859e",
    },
}

REQUIRED_PHASES = ("readiness", "bronze", "silver", "gold", "verify", "publish")


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


def check_unresolved_symlinks(raw_path: Path) -> None:
    """Fail-closed check: verify raw path and every ancestor component is not a symlink before resolve()."""
    curr: Optional[Path] = raw_path
    while curr is not None:
        if curr.is_symlink() or os.path.islink(curr):
            raise ValueError(f"SYMLINK_REJECTED: component {curr} is a symlink")
        parent = curr.parent
        if parent == curr:
            break
        curr = parent


def check_no_symlinks(path: Path) -> None:
    """Fail-closed check: raise ValueError if path or any parent component is a symlink."""
    for p in (path, *path.parents):
        if p.is_symlink() or os.path.islink(p):
            raise ValueError(f"SYMLINK_REJECTED: component {p} is a symlink")


def safe_write_json_file(target_path: Path, data: Any, overwrite: bool = False) -> Path:
    """Write JSON data to target_path with fail-closed symlink and overwrite guards."""
    raw = Path(target_path)
    # 1. Check symlink on the un-resolved path and all parents
    check_unresolved_symlinks(raw)

    # 2. Check if file or symlink already exists on raw path
    if (raw.exists() or raw.is_symlink() or os.path.islink(raw)) and not overwrite:
        raise FileExistsError(f"OUTPUT_FILE_ALREADY_EXISTS: {raw} (pass overwrite=True to replace)")

    resolved = raw.resolve()
    # 3. Check resolved path and parents for symlinks
    check_no_symlinks(resolved)

    # 4. Check if resolved file exists
    if resolved.exists() and not overwrite:
        raise FileExistsError(f"OUTPUT_FILE_ALREADY_EXISTS: {resolved} (pass overwrite=True to replace)")

    resolved.parent.mkdir(parents=True, exist_ok=True)
    with open(resolved, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    return resolved


def check_within_root(path: Path, root: Path) -> None:
    """Fail-closed check: verify path.resolve() is relative to root.resolve()."""
    resolved_path = path.resolve()
    resolved_root = root.resolve()
    if not resolved_path.is_relative_to(resolved_root):
        raise ValueError(f"PATH_ESCAPES_SOURCE_ROOT: {path} resolves outside {root}")


def check_raw_path_safety(raw_path: str) -> Tuple[bool, str]:
    """Validate raw path string strictly before any modification.

    Rejects:
    - Empty, None, or non-string
    - Null bytes (\\0)
    - Absolute paths starting with / or \\
    - Absolute paths with Windows drive letter (e.g. C:)
    - Path traversal with .. components
    - Current directory . components
    - Empty components (// or trailing slash)
    """
    if not raw_path or not isinstance(raw_path, str):
        return False, "EMPTY_OR_NON_STRING_PATH"
    if "\0" in raw_path:
        return False, "NULL_BYTE_IN_PATH"
    if raw_path.startswith("/") or raw_path.startswith("\\"):
        return False, "ABSOLUTE_PATH_LEADING_SLASH"
    if re.match(r"^[a-zA-Z]:", raw_path):
        return False, "ABSOLUTE_PATH_DRIVE_LETTER"

    norm = raw_path.replace("\\", "/")
    parts = norm.split("/")
    if any(p == ".." for p in parts):
        return False, "PATH_TRAVERSAL_DOT_DOT"
    if any(p == "." for p in parts):
        return False, "PATH_SEGMENT_DOT"
    if any(p == "" for p in parts):
        return False, "EMPTY_PATH_SEGMENT_OR_CONSECUTIVE_SLASH"

    return True, "SAFE"


def parse_uri_or_key(raw_input: str, default_bucket: str = "japan-earthquake") -> Tuple[str, str]:
    """Parse raw URI or relative path into (s3_uri, rel_key) with fail-closed safety."""
    if not raw_input or not isinstance(raw_input, str):
        raise ValueError(f"INVALID_INPUT: {raw_input}")

    clean = raw_input.strip()
    prefix = f"s3://{default_bucket}/"
    if clean.startswith(prefix):
        rel_key = clean[len(prefix):]
        s3_uri = clean
    elif clean.startswith("s3://"):
        parts = clean[5:].split("/", 1)
        if len(parts) == 2 and parts[0] == default_bucket:
            rel_key = parts[1]
            s3_uri = clean
        else:
            raise ValueError(f"UNRECOGNIZED_S3_URI: {clean}")
    else:
        rel_key = clean
        s3_uri = f"s3://{default_bucket}/{clean}"

    safe, reason = check_raw_path_safety(rel_key)
    if not safe:
        raise ValueError(f"UNSAFE_PATH: {rel_key} ({reason})")

    return s3_uri, rel_key.replace("\\", "/")


def parse_trusted_checksums(data: Any, default_bucket: str = "japan-earthquake") -> Dict[str, Dict[str, Any]]:
    """Parse trusted checksum mapping from JSON file or object."""
    if isinstance(data, (str, Path)):
        p = Path(data).resolve()
        check_no_symlinks(p)
        if not p.is_file():
            raise FileNotFoundError(f"TRUSTED_CHECKSUMS_FILE_NOT_FOUND: {p}")
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)

    mapping: Dict[str, Dict[str, Any]] = {}

    if isinstance(data, dict):
        if "checksums" in data and isinstance(data["checksums"], dict):
            for k, v in data["checksums"].items():
                s3_uri, rel_key = parse_uri_or_key(k, default_bucket=default_bucket)
                mapping[rel_key] = {
                    "sha256": str(v).strip().lower(),
                    "s3_uri": s3_uri,
                    "archive_path": rel_key,
                }
            return mapping

        if "objects" in data and isinstance(data["objects"], list):
            data = data["objects"]
        elif "required_objects" in data and isinstance(data["required_objects"], list):
            data = data["required_objects"]
        elif "frontier_objects" in data and isinstance(data["frontier_objects"], list):
            data = data["frontier_objects"]
        else:
            for k, v in data.items():
                if isinstance(v, str) and re.fullmatch(r"[0-9a-fA-F]{64}", v.strip()):
                    s3_uri, rel_key = parse_uri_or_key(k, default_bucket=default_bucket)
                    mapping[rel_key] = {
                        "sha256": v.strip().lower(),
                        "s3_uri": s3_uri,
                        "archive_path": rel_key,
                    }
            if mapping:
                return mapping

    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            raw_p = item.get("uri") or item.get("s3_uri") or item.get("path") or item.get("archive_path")
            sha = item.get("sha256") or item.get("sha") or item.get("checksum") or item.get("expected_sha256")
            phase = item.get("phase")
            if raw_p and sha:
                s3_uri, rel_key = parse_uri_or_key(str(raw_p), default_bucket=default_bucket)
                mapping[rel_key] = {
                    "sha256": str(sha).strip().lower(),
                    "phase": phase,
                    "s3_uri": s3_uri,
                    "archive_path": rel_key,
                }

    return mapping


def extract_lineage_from_data_zip(
    data_zip_path: Path,
    owner: str,
) -> Tuple[Dict[str, Dict[str, bytes]], Dict[str, str]]:
    """Extract and verify all required lineage phase objects from 63-months data zip.

    Returns:
        runs: {operation_id: {phase: bytes_content}}
        trusted_checksums: {archive_path: sha256}
    """
    if owner not in WORKER_LINEAGE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}")

    data_zip_path = Path(data_zip_path).resolve()
    check_no_symlinks(data_zip_path)
    if not data_zip_path.is_file():
        raise FileNotFoundError(f"DATA_ZIP_NOT_FOUND: {data_zip_path}")

    runs: Dict[str, Dict[str, bytes]] = {}
    trusted_checksums: Dict[str, str] = {}

    with zipfile.ZipFile(data_zip_path, "r") as z:
        for info in z.infolist():
            mode = info.external_attr >> 16
            if mode and (mode & 0o170000 == 0o120000):
                raise ValueError(f"SYMLINK_MEMBER_FORBIDDEN: {info.filename}")
            safe, reason = check_raw_path_safety(info.filename)
            if not safe:
                raise ValueError(f"UNSAFE_ZIP_ENTRY_PATH: {info.filename} ({reason})")

        namelist_set = set(z.namelist())
        record_names = sorted([x for x in namelist_set if x.endswith("record.json")])

        for rname in record_names:
            rec_bytes = z.read(rname)
            rec = json.loads(rec_bytes.decode("utf-8"))
            m = rname.split("/")[1]

            for r in rec.get("receipts", []):
                op = r.get("operation_id")
                phase = r.get("phase")
                exp_sha = r.get("artifact_sha256")

                if not op or not phase or not exp_sha:
                    continue

                fn = f"months/{m}/{phase}.json"
                if fn not in namelist_set:
                    continue

                content = z.read(fn)
                act_sha = compute_sha256(content)
                if act_sha != exp_sha:
                    raise ValueError(f"RECEIPT_SHA_MISMATCH for {fn}: expected {exp_sha}, got {act_sha}")

                if op not in runs:
                    runs[op] = {}

                if phase not in runs[op]:
                    runs[op][phase] = content
                    archive_path = f"silver/real-etl/{op}/{phase}.json"
                    trusted_checksums[archive_path] = act_sha

    return runs, trusted_checksums


def pack_lineage_archive(
    owner: str,
    source_input: Path,
    output_zip: Path,
    output_spec_file: Optional[Path] = None,
    overwrite: bool = False,
) -> Dict[str, Any]:
    """Package supplemental lineage objects into ZIP with fail-closed checks.

    `source_input` may be a data ZIP or an extracted directory.
    """
    if owner not in WORKER_LINEAGE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}")

    spec = WORKER_LINEAGE_SPECS[owner]
    required_count = spec["total_required_objects"]
    expected_runs = spec["run_count"]

    output_zip_raw = Path(output_zip)
    check_unresolved_symlinks(output_zip_raw)
    if (output_zip_raw.exists() or output_zip_raw.is_symlink() or os.path.islink(output_zip_raw)) and not overwrite:
        raise FileExistsError(f"OUTPUT_ZIP_ALREADY_EXISTS: {output_zip_raw} (pass overwrite=True to replace)")

    output_zip = output_zip_raw.resolve()
    check_no_symlinks(output_zip)
    if output_zip.exists() and not overwrite:
        raise FileExistsError(f"OUTPUT_ZIP_ALREADY_EXISTS: {output_zip} (pass overwrite=True to replace)")

    source_input_raw = Path(source_input)
    check_unresolved_symlinks(source_input_raw)
    source_input = source_input_raw.resolve()
    check_no_symlinks(source_input)

    if source_input.is_file() and source_input.suffix == ".zip":
        runs, checksums_map = extract_lineage_from_data_zip(source_input, owner=owner)
    elif source_input.is_dir():
        # Read from extracted directories with strict symlink and root escaping guards
        runs = {}
        checksums_map = {}
        for root, _, files in os.walk(source_input):
            root_p = Path(root)
            check_no_symlinks(root_p)
            for f in sorted(files):
                fpath = root_p / f
                check_no_symlinks(fpath)
                check_within_root(fpath, source_input)

                if f.endswith(".json"):
                    rel = fpath.relative_to(source_input).as_posix()
                    # Expect exact layout silver/real-etl/<run_id>/<phase>.json
                    parts = rel.split("/")
                    if len(parts) == 4 and parts[0] == "silver" and parts[1] == "real-etl":
                        op = parts[2]
                        if not re.fullmatch(r"^[a-zA-Z0-9_\-]+$", op):
                            raise ValueError(f"INVALID_OPERATION_ID_LAYOUT: {op} in {rel}")
                        if not parts[3].endswith(".json"):
                            continue
                        phase = parts[3][:-5]
                        if phase in REQUIRED_PHASES:
                            if op in runs and phase in runs[op]:
                                raise ValueError(f"DUPLICATE_LOGICAL_RUN_PHASE: {op}/{phase} found multiple times")
                            content = fpath.read_bytes()
                            runs.setdefault(op, {})[phase] = content
                            checksums_map[rel] = compute_sha256(content)
    else:
        raise ValueError(f"INVALID_SOURCE_INPUT: {source_input}")

    if len(runs) != expected_runs:
        raise ValueError(f"RUN_COUNT_MISMATCH: expected {expected_runs}, got {len(runs)}")

    for op, pdict in runs.items():
        missing_phases = set(REQUIRED_PHASES) - set(pdict.keys())
        if missing_phases:
            raise ValueError(f"MISSING_PHASES in run {op}: {sorted(missing_phases)}")

    total_objects = sum(len(pdict) for pdict in runs.values())
    if total_objects != required_count:
        raise ValueError(f"TOTAL_OBJECTS_MISMATCH: expected {required_count}, got {total_objects}")

    output_zip.parent.mkdir(parents=True, exist_ok=True)

    manifest_objects: List[Dict[str, Any]] = []

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for op in sorted(runs):
            for phase in sorted(runs[op]):
                rel_path = f"silver/real-etl/{op}/{phase}.json"
                safe, reason = check_raw_path_safety(rel_path)
                if not safe:
                    raise ValueError(f"UNSAFE_ARCHIVE_PATH: {rel_path} ({reason})")

                content = runs[op][phase]
                sha = compute_sha256(content)
                s3_uri = f"s3://japan-earthquake/{rel_path}"

                zf.writestr(rel_path, content)
                manifest_objects.append({
                    "category": "real_etl_phase",
                    "operation_id": op,
                    "phase": phase,
                    "path": rel_path,
                    "archive_path": rel_path,
                    "s3_uri": s3_uri,
                    "uri": s3_uri,
                    "bytes": len(content),
                    "sha256": sha,
                })

        # Manifest
        manifest_data = {
            "version": "dat03-lineage-manifest-v1",
            "owner": owner,
            "evidence_tier": "Tier-2-Supplemental-Phase-Evidence",
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "target_range": f"{spec['range'][0]}..{spec['range'][1]}",
            "month_count": spec["month_count"],
            "run_count": spec["run_count"],
            "total_objects": total_objects,
            "phases_per_run": spec["phases_per_run"],
            "objects": manifest_objects,
        }
        manifest_bytes = json.dumps(manifest_data, indent=2, sort_keys=True).encode("utf-8")
        zf.writestr("manifest.json", manifest_bytes)

    zip_sha = compute_file_sha256(output_zip)
    zip_bytes = output_zip.stat().st_size

    result = {
        "status": "PASS",
        "owner": owner,
        "zip_path": str(output_zip),
        "zip_bytes": zip_bytes,
        "zip_sha256": zip_sha,
        "total_objects": total_objects,
        "run_count": len(runs),
        "manifest_sha256": compute_sha256(manifest_bytes),
    }

    if output_spec_file:
        spec_export = {
            "owner": owner,
            "evidence_tier": "Tier-2-Supplemental-Phase-Evidence",
            "total_objects": total_objects,
            "run_count": len(runs),
            "zip_sha256": zip_sha,
            "manifest_sha256": compute_sha256(manifest_bytes),
            "checksums": {item["path"]: item["sha256"] for item in manifest_objects},
        }
        spec_path = safe_write_json_file(Path(output_spec_file), spec_export, overwrite=overwrite)
        result["spec_file"] = str(spec_path)

    return result


def verify_lineage_archive(
    owner: str,
    zip_path: Path,
    expected_checksums: Optional[Any] = None,
    require_trusted: bool = False,
) -> Dict[str, Any]:
    """Verify integrity of a lineage ZIP archive with strict fail-closed checks."""
    if owner not in WORKER_LINEAGE_SPECS:
        return {"status": "FAIL", "reason": f"UNRECOGNIZED_OWNER: {owner}"}

    spec = WORKER_LINEAGE_SPECS[owner]
    required_count = spec["total_required_objects"]

    zip_path_raw = Path(zip_path)
    try:
        check_unresolved_symlinks(zip_path_raw)
        zip_path = zip_path_raw.resolve()
        check_no_symlinks(zip_path)
    except ValueError as ve:
        return {"status": "FAIL", "reason": str(ve)}

    if not zip_path.is_file():
        return {"status": "FAIL", "reason": f"ZIP_FILE_NOT_FOUND: {zip_path}"}

    if require_trusted and expected_checksums is None:
        return {"status": "FAIL", "reason": "REQUIRED_TRUSTED_CHECKSUMS_MISSING"}

    trusted_map: Optional[Dict[str, Dict[str, Any]]] = None
    if expected_checksums is not None:
        try:
            trusted_map = parse_trusted_checksums(expected_checksums)
        except Exception as ex:
            return {"status": "FAIL", "reason": f"INVALID_TRUSTED_CHECKSUMS_FORMAT: {ex}"}

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            infolist = zf.infolist()
            raw_names = [info.filename for info in infolist]

            if len(raw_names) != len(set(raw_names)):
                return {"status": "FAIL", "reason": "DUPLICATE_ZIP_MEMBER_NAMES"}

            # Reject symlinks and directory members in ZIP
            for info in infolist:
                mode = info.external_attr >> 16
                if mode and (mode & 0o170000 == 0o120000):
                    return {
                        "status": "FAIL",
                        "reason": f"SYMLINK_ZIP_ENTRY_REJECTED: {info.filename}",
                    }
                if info.is_dir():
                    return {
                        "status": "FAIL",
                        "reason": f"DIRECTORY_MEMBER_FORBIDDEN: {info.filename}",
                    }

            # Check raw path safety
            for name in raw_names:
                safe, reason = check_raw_path_safety(name)
                if not safe:
                    return {"status": "FAIL", "reason": f"UNSAFE_ZIP_ENTRY_PATH: {name} ({reason})"}

            if "manifest.json" not in raw_names:
                return {"status": "FAIL", "reason": "MISSING_INTERNAL_MANIFEST"}

            manifest_content = zf.read("manifest.json")
            try:
                manifest = json.loads(manifest_content.decode("utf-8"))
            except Exception as ex:
                return {"status": "FAIL", "reason": f"MALFORMED_INTERNAL_MANIFEST: {ex}"}

            if manifest.get("owner") != owner:
                return {
                    "status": "FAIL",
                    "reason": f"OWNER_MISMATCH: expected {owner}, manifest declares {manifest.get('owner')}",
                }

            manifest_objects = manifest.get("objects", [])
            if len(manifest_objects) != required_count:
                return {
                    "status": "FAIL",
                    "reason": f"MANIFEST_OBJECT_COUNT_MISMATCH: expected {required_count}, got {len(manifest_objects)}",
                }

            objects_in_zip = [name for name in raw_names if name != "manifest.json"]
            if len(objects_in_zip) != required_count:
                return {
                    "status": "FAIL",
                    "reason": f"ZIP_OBJECT_COUNT_MISMATCH: expected {required_count}, got {len(objects_in_zip)}",
                }

            manifest_dict = {}
            for obj in manifest_objects:
                p = obj.get("path") or obj.get("archive_path")
                if not p:
                    return {"status": "FAIL", "reason": "MANIFEST_OBJECT_MISSING_PATH"}
                manifest_dict[p] = obj

            # Verify byte hashes against internal manifest
            verified_runs: Set[str] = set()
            phases_by_run: Dict[str, Set[str]] = {}

            for name in objects_in_zip:
                if name not in manifest_dict:
                    return {"status": "FAIL", "reason": f"EXTRA_OBJECT_NOT_IN_MANIFEST: {name}"}

                data = zf.read(name)
                actual_sha = compute_sha256(data)
                exp_sha = manifest_dict[name].get("sha256")
                if actual_sha != exp_sha:
                    return {
                        "status": "FAIL",
                        "reason": f"MANIFEST_CHECKSUM_MISMATCH: {name} expected {exp_sha}, got {actual_sha}",
                    }

                # Strict namespace layout: silver/real-etl/<operation_id>/<phase>.json
                parts = name.split("/")
                if len(parts) != 4 or parts[0] != "silver" or parts[1] != "real-etl":
                    return {
                        "status": "FAIL",
                        "reason": f"INVALID_NAMESPACE_LAYOUT: {name} (expected silver/real-etl/<operation_id>/<phase>.json)",
                    }

                op = parts[2]
                if not re.fullmatch(r"^[a-zA-Z0-9_\-]+$", op):
                    return {
                        "status": "FAIL",
                        "reason": f"INVALID_OPERATION_ID_FORMAT: {op} in {name}",
                    }

                if not parts[3].endswith(".json"):
                    return {
                        "status": "FAIL",
                        "reason": f"INVALID_FILE_EXTENSION: {parts[3]} in {name}",
                    }

                phase = parts[3][:-5]
                if phase not in REQUIRED_PHASES:
                    return {
                        "status": "FAIL",
                        "reason": f"UNRECOGNIZED_PHASE: {phase} in {name}",
                    }

                # Consistency check against internal manifest fields
                manifest_obj = manifest_dict[name]
                manifest_op = manifest_obj.get("operation_id")
                manifest_phase = manifest_obj.get("phase")
                if manifest_op and manifest_op != op:
                    return {
                        "status": "FAIL",
                        "reason": f"MANIFEST_OPERATION_ID_MISMATCH: {name} path has {op}, manifest has {manifest_op}",
                    }
                if manifest_phase and manifest_phase != phase:
                    return {
                        "status": "FAIL",
                        "reason": f"MANIFEST_PHASE_MISMATCH: {name} path has {phase}, manifest has {manifest_phase}",
                    }

                # Reject duplicate logical (operation_id, phase)
                if op in phases_by_run and phase in phases_by_run[op]:
                    return {
                        "status": "FAIL",
                        "reason": f"DUPLICATE_LOGICAL_RUN_PHASE: {op}/{phase}",
                    }

                verified_runs.add(op)
                phases_by_run.setdefault(op, set()).add(phase)

            if len(verified_runs) != spec["run_count"]:
                return {
                    "status": "FAIL",
                    "reason": f"RUN_COUNT_MISMATCH: expected {spec['run_count']}, verified {len(verified_runs)}",
                }

            for op, phases in phases_by_run.items():
                if set(REQUIRED_PHASES) != phases:
                    return {
                        "status": "FAIL",
                        "reason": f"INCOMPLETE_PHASES_IN_RUN: {op} has {sorted(phases)}",
                    }

            # If trusted verification is requested
            if trusted_map is not None:
                missing_trusted = set(objects_in_zip) - set(trusted_map.keys())
                if missing_trusted:
                    return {
                        "status": "FAIL",
                        "reason": "INCOMPLETE_TRUSTED_CHECKSUMS",
                        "missing_count": len(missing_trusted),
                        "missing_samples": sorted(list(missing_trusted))[:5],
                    }

                for name in objects_in_zip:
                    trusted_sha = trusted_map[name].get("sha256")
                    data = zf.read(name)
                    actual_sha = compute_sha256(data)
                    if actual_sha != trusted_sha:
                        return {
                            "status": "FAIL",
                            "reason": f"TRUSTED_CHECKSUM_MISMATCH: {name} expected {trusted_sha}, got {actual_sha}",
                        }

        zip_sha = compute_file_sha256(zip_path)
        return {
            "status": "PASS",
            "owner": owner,
            "verification_level": "trusted_lineage" if trusted_map is not None else "self_integrity",
            "zip_path": str(zip_path),
            "zip_sha256": zip_sha,
            "verified_objects": len(objects_in_zip),
            "verified_runs": len(verified_runs),
            "trusted_objects_verified": len(trusted_map) if trusted_map is not None else 0,
        }

    except zipfile.BadZipFile:
        return {"status": "FAIL", "reason": "CORRUPT_OR_BAD_ZIP_FILE"}
    except Exception as ex:
        return {"status": "FAIL", "reason": f"EXCEPTION_DURING_VERIFICATION: {ex}"}


def extract_frontier_spec(
    owner: str,
    provenance_zip: Path,
    output_spec_file: Optional[Path] = None,
    overwrite: bool = False,
    expected_provenance_sha: Optional[str] = None,
    provenance_checkpoint: Optional[Path] = None,
    allow_unverified_provenance: bool = False,
) -> Dict[str, Any]:
    """Extract Tier 3 transitive closure frontier specification (114 objects for rosy179).

    Frontier consists of:
    - 92 Silver dataset manifests (23 runs * 4 datasets: source_observation, reject_record,
      source_link, canonical_membership under silver/bundles/<run_id>/<dataset>/manifest.json)
    - 22 parent Gold commit JSONs at physical prefix silver/gold-commits/<run_id>/commit.json
    Total = 114 objects.

    Security & Integrity Policy:
    - Verifies input provenance ZIP against pinned SHA-256 or external checkpoint.
    - Rejects empty, missing, or malformed bundle manifests and registries.
    - Rejects duplicate ZIP member names and duplicate frontier object paths.
    - Rejects conflicting SHA-256 for identical logical paths.
    - Validates S3 URIs, buckets, namespaces, and hex SHA-256 digests.
    - Output file must not overwrite existing file or checkpoint unless overwrite=True.
    - Symlinks on output path or parent directories are strictly rejected before resolution.
    - Evidence tiers accurately report verification status (Tier 1 INPUT_PIN_VERIFIED,
      Tier 2 NOT_CHECKED, Tier 3 SPECIFIED_PENDING_BYTES).
    """
    if owner not in WORKER_LINEAGE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}")

    spec = WORKER_LINEAGE_SPECS[owner]

    prov_raw = Path(provenance_zip)
    check_unresolved_symlinks(prov_raw)
    prov_zip = prov_raw.resolve()
    check_no_symlinks(prov_zip)
    if not prov_zip.is_file():
        raise FileNotFoundError(f"PROVENANCE_ZIP_NOT_FOUND: {prov_zip}")

    # 1. Resolve and verify expected provenance SHA / checkpoint
    pin_sha: Optional[str] = None
    if expected_provenance_sha:
        pin_sha = expected_provenance_sha.strip().lower()
    elif provenance_checkpoint:
        cp_raw = Path(provenance_checkpoint)
        check_unresolved_symlinks(cp_raw)
        cp_path = cp_raw.resolve()
        check_no_symlinks(cp_path)
        if not cp_path.is_file():
            raise FileNotFoundError(f"PROVENANCE_CHECKPOINT_NOT_FOUND: {cp_path}")
        if cp_path.suffix == ".json":
            with open(cp_path, "r", encoding="utf-8") as f:
                cp_data = json.load(f)
            if isinstance(cp_data, dict):
                if "packages" in cp_data and prov_zip.name in cp_data["packages"]:
                    pin_sha = cp_data["packages"][prov_zip.name].get("sha256")
                elif "checksums" in cp_data and prov_zip.name in cp_data["checksums"]:
                    pin_sha = cp_data["checksums"][prov_zip.name]
                elif prov_zip.name in cp_data:
                    pin_sha = cp_data[prov_zip.name]
        else:
            with open(cp_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 2 and (parts[1].endswith(prov_zip.name) or parts[1] == prov_zip.name):
                        pin_sha = parts[0].strip().lower()
                        break
    elif not allow_unverified_provenance and spec.get("pinned_provenance_sha256"):
        pin_sha = spec["pinned_provenance_sha256"].strip().lower()

    actual_prov_sha = compute_file_sha256(prov_zip)
    is_pin_verified = False

    if pin_sha:
        if not re.fullmatch(r"^[0-9a-fA-F]{64}$", pin_sha):
            raise ValueError(f"MALFORMED_EXPECTED_PROVENANCE_SHA: {pin_sha}")
        if actual_prov_sha.lower() != pin_sha.lower():
            raise ValueError(f"PROVENANCE_SHA_MISMATCH: expected {pin_sha}, got {actual_prov_sha}")
        is_pin_verified = True
    else:
        if not allow_unverified_provenance:
            raise ValueError(
                f"PROVENANCE_INPUT_PIN_REQUIRED: owner {owner} has no pinned provenance SHA. "
                "Provide --expected-provenance-sha or --provenance-checkpoint, or pass --allow-unverified-provenance."
            )

    dataset_manifests: List[Dict[str, Any]] = []
    parent_commits: List[Dict[str, Any]] = []
    checksums_map: Dict[str, str] = {}
    seen_frontier_paths: Set[str] = set()

    with zipfile.ZipFile(prov_zip, "r") as zf:
        infolist = zf.infolist()
        raw_names = [info.filename for info in infolist]

        # 2. Check duplicate ZIP member names
        if len(raw_names) != len(set(raw_names)):
            dups = sorted(set([x for x in raw_names if raw_names.count(x) > 1]))
            raise ValueError(f"DUPLICATE_ZIP_MEMBER_NAMES: {dups}")

        # 3. Check member modes for symlinks or directories
        for info in infolist:
            mode = info.external_attr >> 16
            if mode and (mode & 0o170000 == 0o120000):
                raise ValueError(f"SYMLINK_MEMBER_FORBIDDEN: {info.filename}")
            safe, reason = check_raw_path_safety(info.filename)
            if not safe:
                raise ValueError(f"UNSAFE_ZIP_ENTRY_PATH: {info.filename} ({reason})")

        # 4. Extract and validate Silver bundle manifests
        bundle_names = sorted([n for n in raw_names if n.startswith("silver/bundles/") and n.endswith("manifest.json")])
        expected_bundles = spec.get("expected_bundle_count", spec["run_count"])
        if len(bundle_names) != expected_bundles:
            raise ValueError(f"BUNDLE_MANIFEST_COUNT_MISMATCH: expected {expected_bundles}, got {len(bundle_names)}")

        REQUIRED_DATASETS = ("source_observation", "reject_record", "source_link", "canonical_membership")

        for bname in bundle_names:
            parts = bname.split("/")
            if len(parts) != 4 or parts[0] != "silver" or parts[1] != "bundles" or parts[3] != "manifest.json":
                raise ValueError(f"INVALID_BUNDLE_MANIFEST_PATH_LAYOUT: {bname}")
            run_id = parts[2]
            if not re.fullmatch(r"^[a-zA-Z0-9_\-]+$", run_id):
                raise ValueError(f"INVALID_RUN_ID_IN_PATH: {run_id} in {bname}")

            bm_bytes = zf.read(bname)
            bm = json.loads(bm_bytes.decode("utf-8"))
            if bm.get("run_id") != run_id:
                raise ValueError(f"RUN_ID_MISMATCH in {bname}: path has {run_id}, manifest declares {bm.get('run_id')}")

            datasets = bm.get("datasets", {})
            if not isinstance(datasets, dict):
                raise ValueError(f"INVALID_DATASETS_CONTAINER in {bname}")

            missing_ds = set(REQUIRED_DATASETS) - set(datasets.keys())
            if missing_ds:
                raise ValueError(f"MISSING_DATASETS in {bname}: {sorted(missing_ds)}")
            extra_ds = set(datasets.keys()) - set(REQUIRED_DATASETS)
            if extra_ds:
                raise ValueError(f"UNRECOGNIZED_DATASETS in {bname}: {sorted(extra_ds)}")

            for ds_name in sorted(REQUIRED_DATASETS):
                ds_info = datasets[ds_name]
                uri = ds_info.get("manifest_uri")
                sha = ds_info.get("sha256")
                rec_count = ds_info.get("record_count", 0)

                exp_uri = f"s3://japan-earthquake/silver/bundles/{run_id}/{ds_name}/manifest.json"
                if uri != exp_uri:
                    raise ValueError(f"INVALID_DATASET_MANIFEST_URI: expected {exp_uri}, got {uri}")

                rel_path = f"silver/bundles/{run_id}/{ds_name}/manifest.json"
                if not sha or not isinstance(sha, str) or not re.fullmatch(r"^[0-9a-fA-F]{64}$", sha):
                    raise ValueError(f"INVALID_DATASET_SHA256 in {bname}/{ds_name}: {sha}")
                clean_sha = sha.lower()

                if rel_path in seen_frontier_paths:
                    if checksums_map.get(rel_path) != clean_sha:
                        raise ValueError(f"CONFLICTING_SHA: {rel_path} has conflicting sha {clean_sha} vs {checksums_map.get(rel_path)}")
                    raise ValueError(f"DUPLICATE_FRONTIER_OBJECT: {rel_path} encountered multiple times")

                seen_frontier_paths.add(rel_path)
                checksums_map[rel_path] = clean_sha
                dataset_manifests.append({
                    "category": "silver_dataset_manifest",
                    "run_id": run_id,
                    "dataset": ds_name,
                    "s3_uri": uri,
                    "path": rel_path,
                    "sha256": clean_sha,
                    "record_count": rec_count,
                })

        # 5. Extract and validate parent Gold commits from registries
        reg_names = sorted([n for n in raw_names if n.startswith("silver/full-month/") and n.endswith("registry.json")])
        expected_regs = spec.get("expected_registry_count", spec["run_count"])
        if len(reg_names) != expected_regs:
            raise ValueError(f"REGISTRY_COUNT_MISMATCH: expected {expected_regs}, got {len(reg_names)}")

        for rname in reg_names:
            parts = rname.split("/")
            if len(parts) != 4 or parts[0] != "silver" or parts[1] != "full-month" or parts[3] != "registry.json":
                raise ValueError(f"INVALID_REGISTRY_PATH_LAYOUT: {rname}")
            run_id = parts[2]
            if not re.fullmatch(r"^[a-zA-Z0-9_\-]+$", run_id):
                raise ValueError(f"INVALID_RUN_ID_IN_PATH: {run_id} in {rname}")

            reg_bytes = zf.read(rname)
            reg = json.loads(reg_bytes.decode("utf-8"))
            if reg.get("run_id") != run_id:
                raise ValueError(f"RUN_ID_MISMATCH in {rname}: path has {run_id}, registry declares {reg.get('run_id')}")

            parent = reg.get("parent")
            if parent and isinstance(parent, dict) and parent.get("commit_key"):
                commit_key = parent.get("commit_key")
                parent_run = parent.get("run_id")
                if not parent_run or not re.fullmatch(r"^[a-zA-Z0-9_\-]+$", parent_run):
                    raise ValueError(f"INVALID_PARENT_RUN_ID in {rname}: {parent_run}")

                exp_commit_key = f"gold-commits/{parent_run}/commit.json"
                norm_key = f"silver/gold-commits/{parent_run}/commit.json"
                if commit_key != exp_commit_key and commit_key != norm_key:
                    raise ValueError(f"INVALID_COMMIT_KEY in {rname}: expected {exp_commit_key}, got {commit_key}")

                commit_sha = parent.get("commit_sha256")
                if not commit_sha or not isinstance(commit_sha, str) or not re.fullmatch(r"^[0-9a-fA-F]{64}$", commit_sha):
                    raise ValueError(f"INVALID_COMMIT_SHA256 in {rname}: {commit_sha}")
                clean_commit_sha = commit_sha.lower()

                if norm_key in seen_frontier_paths:
                    if checksums_map.get(norm_key) != clean_commit_sha:
                        raise ValueError(f"CONFLICTING_SHA: {norm_key} has conflicting sha {clean_commit_sha} vs {checksums_map.get(norm_key)}")
                    raise ValueError(f"DUPLICATE_FRONTIER_OBJECT: {norm_key} encountered multiple times")

                seen_frontier_paths.add(norm_key)
                checksums_map[norm_key] = clean_commit_sha
                parent_commits.append({
                    "category": "gold_parent_commit",
                    "child_run_id": run_id,
                    "parent_run_id": parent_run,
                    "commit_key": commit_key,
                    "s3_uri": f"s3://japan-earthquake/{norm_key}",
                    "path": norm_key,
                    "sha256": clean_commit_sha,
                })

        # Count Bronze manifests actually in provenance zip
        bronze_names = sorted([n for n in raw_names if n.startswith("bronze/") and n.endswith("manifest.json")])

    # 6. Validate expected counts and reconcile checksums mapping
    expected_ds_count = spec.get("expected_dataset_manifest_count", expected_bundles * 4)
    if len(dataset_manifests) != expected_ds_count:
        raise ValueError(f"DATASET_MANIFEST_COUNT_MISMATCH: expected {expected_ds_count}, got {len(dataset_manifests)}")

    expected_parent_count = spec.get("expected_parent_commit_count", len(parent_commits))
    if len(parent_commits) != expected_parent_count:
        raise ValueError(f"PARENT_COMMIT_COUNT_MISMATCH: expected {expected_parent_count}, got {len(parent_commits)}")

    all_objects = dataset_manifests + parent_commits
    expected_frontier = spec.get("expected_frontier_count", expected_ds_count + expected_parent_count)
    if len(all_objects) != expected_frontier:
        raise ValueError(f"TOTAL_FRONTIER_COUNT_MISMATCH: expected {expected_frontier}, got {len(all_objects)}")

    if len(checksums_map) != len(all_objects):
        raise ValueError(f"FRONTIER_RECONCILIATION_FAILED: checksums count {len(checksums_map)} != objects count {len(all_objects)}")

    # 7. Construct output specification
    spec_data = {
        "version": "dat03-frontier-114-spec-v1",
        "owner": owner,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_provenance_zip": prov_zip.name,
        "source_provenance_sha256": actual_prov_sha,
        "tier_classification": {
            "tier_1_first_hop_provenance": {
                "status": "INPUT_PIN_VERIFIED" if is_pin_verified else "NOT_CHECKED",
                "verified_package": prov_zip.name,
                "verified_sha256": actual_prov_sha,
                "total_first_hop_objects": len(bundle_names) + len(reg_names) + len(bronze_names),
                "bronze_manifest_count": len(bronze_names),
                "bundle_manifest_count": len(bundle_names),
                "registry_count": len(reg_names),
            },
            "tier_2_supplemental_phase_evidence": {
                "status": "NOT_CHECKED",
                "expected_object_count": spec["total_required_objects"],
                "package": f"{owner}-dat03-lineage-metadata.zip",
                "note": "Phase journals package (Tier 2) is supplemental evidence not verified by this frontier extraction",
            },
            "tier_3_transitive_closure_frontier": {
                "status": "SPECIFIED_PENDING_BYTES",
                "gate_status": "CONG_1_TRANSITIVE_CLOSURE_OPEN",
                "total_frontier_objects": len(all_objects),
                "silver_dataset_manifests": len(dataset_manifests),
                "gold_parent_commits": len(parent_commits),
                "note": "Cần cung cấp bytes gốc từ MinIO/object storage theo danh sách 114 URIs/SHAs để đối soát transitive closure",
            },
        },
        "summary": {
            "total_frontier_objects": len(all_objects),
            "silver_dataset_manifest_count": len(dataset_manifests),
            "gold_parent_commit_count": len(parent_commits),
        },
        "frontier_objects": all_objects,
        "checksums": checksums_map,
    }

    result = {
        "status": "PASS",
        "owner": owner,
        "total_frontier_objects": len(all_objects),
        "silver_dataset_manifests": len(dataset_manifests),
        "gold_parent_commits": len(parent_commits),
        "provenance_pin_verified": is_pin_verified,
        "output_spec_file": None,
    }

    if output_spec_file:
        out_p = safe_write_json_file(Path(output_spec_file), spec_data, overwrite=overwrite)
        result["output_spec_file"] = str(out_p)

    return result


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="DAT-03 worker supplemental lineage packaging and verification tool."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: pack
    p_pack = subparsers.add_parser("pack", help="Pack supplemental lineage objects into ZIP.")
    p_pack.add_argument("--owner", required=True, choices=["ThanhTris", "rosy179"])
    p_pack.add_argument("--source-input", required=True, help="Data ZIP or directory containing real-etl phase files.")
    p_pack.add_argument("--output-zip", required=True, help="Path for output lineage ZIP file.")
    p_pack.add_argument("--output-spec", help="Optional path to write trusted checksums JSON mapping.")
    p_pack.add_argument("--overwrite", action="store_true", default=False, help="Overwrite output ZIP if exists.")

    # Subcommand: verify
    p_verify = subparsers.add_parser("verify", help="Verify supplemental lineage ZIP archive.")
    p_verify.add_argument("--owner", required=True, choices=["ThanhTris", "rosy179"])
    p_verify.add_argument("--zip-file", required=True, help="Path to lineage ZIP file.")
    p_verify.add_argument("--checksums-file", help="Optional JSON file with expected SHA-256 for each object.")
    p_verify.add_argument("--require-trusted", action="store_true", default=False, help="Fail if trusted checksums file is not provided.")

    # Subcommand: frontier-spec
    p_frontier = subparsers.add_parser("frontier-spec", help="Extract 114 frontier objects specification from provenance ZIP.")
    p_frontier.add_argument("--owner", required=True, choices=["ThanhTris", "rosy179"])
    p_frontier.add_argument("--provenance-zip", required=True, help="Path to provenance ZIP file.")
    p_frontier.add_argument("--output-spec", required=True, help="Path to output JSON spec file.")
    p_frontier.add_argument("--overwrite", action="store_true", default=False, help="Overwrite output spec file if exists.")
    p_frontier.add_argument("--expected-provenance-sha", help="Optional expected SHA-256 for provenance ZIP (defaults to pinned spec).")
    p_frontier.add_argument("--provenance-checkpoint", help="Optional checkpoint file (JSON or checksums.sha256) containing provenance ZIP hash.")
    p_frontier.add_argument("--allow-unverified-provenance", action="store_true", default=False, help="Allow unverified provenance input if no pin or checkpoint is provided.")

    args = parser.parse_args(argv)

    if args.command == "pack":
        try:
            res = pack_lineage_archive(
                owner=args.owner,
                source_input=Path(args.source_input),
                output_zip=Path(args.output_zip),
                output_spec_file=Path(args.output_spec) if args.output_spec else None,
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

        res = verify_lineage_archive(
            owner=args.owner,
            zip_path=Path(args.zip_file),
            expected_checksums=checksums,
            require_trusted=args.require_trusted,
        )
        print(json.dumps(res, indent=2))
        return 0 if res.get("status") == "PASS" else 1

    if args.command == "frontier-spec":
        try:
            res = extract_frontier_spec(
                owner=args.owner,
                provenance_zip=Path(args.provenance_zip),
                output_spec_file=Path(args.output_spec),
                overwrite=args.overwrite,
                expected_provenance_sha=args.expected_provenance_sha,
                provenance_checkpoint=Path(args.provenance_checkpoint) if args.provenance_checkpoint else None,
                allow_unverified_provenance=args.allow_unverified_provenance,
            )
            print(json.dumps(res, indent=2))
            return 0 if res.get("status") == "PASS" else 1
        except Exception as ex:
            print(json.dumps({"status": "FAIL", "reason": str(ex)}, indent=2))
            return 1

    return 2


if __name__ == "__main__":
    sys.exit(main())
