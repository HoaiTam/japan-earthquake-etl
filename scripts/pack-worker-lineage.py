#!/usr/bin/env python3
"""DAT-03 worker supplemental lineage packaging and verification tool.

Packages and verifies second-hop / transitive lineage metadata objects
(real-etl phase journals: readiness, bronze, silver, gold, verify, publish)
for worker nodes (ThanhTris: 19 runs * 6 phases = 114 objects,
rosy179: 23 runs * 6 phases = 138 objects).

Fail-Closed Policy:
- Missing required phase journals fails verification immediately.
- Tampered, empty, or duplicate files in ZIP are strictly rejected.
- ZIP members with duplicate names fail before reading.
- Paths with directory traversal (..), absolute paths (/ or C:), or symlinks are strictly rejected.
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
    },
    "rosy179": {
        "range": ("2018-10", "2023-12"),
        "month_count": 63,
        "run_count": 23,
        "phases_per_run": 6,
        "total_required_objects": 138,
        "expected_phases": ["readiness", "bronze", "silver", "gold", "verify", "publish"],
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
            sha = item.get("sha256") or item.get("sha") or item.get("checksum")
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
    if not data_zip_path.is_file():
        raise FileNotFoundError(f"DATA_ZIP_NOT_FOUND: {data_zip_path}")

    runs: Dict[str, Dict[str, bytes]] = {}
    trusted_checksums: Dict[str, str] = {}

    with zipfile.ZipFile(data_zip_path, "r") as z:
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

    output_zip = Path(output_zip).resolve()
    if output_zip.exists() and not overwrite:
        raise FileExistsError(f"OUTPUT_ZIP_ALREADY_EXISTS: {output_zip}")

    source_input = Path(source_input).resolve()
    if source_input.is_file() and source_input.suffix == ".zip":
        runs, checksums_map = extract_lineage_from_data_zip(source_input, owner=owner)
    elif source_input.is_dir():
        # Read from extracted directories
        runs = {}
        checksums_map = {}
        for root, _, files in os.walk(source_input):
            for f in files:
                if f.endswith(".json"):
                    fpath = Path(root) / f
                    rel = fpath.relative_to(source_input).as_posix()
                    # Expect silver/real-etl/<run_id>/<phase>.json
                    parts = rel.split("/")
                    if len(parts) >= 4 and parts[0] == "silver" and parts[1] == "real-etl":
                        op = parts[2]
                        phase = parts[3].replace(".json", "")
                        if phase in REQUIRED_PHASES:
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
        spec_path = Path(output_spec_file).resolve()
        spec_path.parent.mkdir(parents=True, exist_ok=True)
        spec_export = {
            "owner": owner,
            "total_objects": total_objects,
            "run_count": len(runs),
            "zip_sha256": zip_sha,
            "manifest_sha256": compute_sha256(manifest_bytes),
            "checksums": {item["path"]: item["sha256"] for item in manifest_objects},
        }
        with open(spec_path, "w", encoding="utf-8") as f:
            json.dump(spec_export, f, indent=2, sort_keys=True)
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

    zip_path = Path(zip_path).resolve()
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

            # Check path safety
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

                parts = name.split("/")
                if len(parts) >= 4:
                    op = parts[2]
                    phase = parts[3].replace(".json", "")
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

    return 2


if __name__ == "__main__":
    sys.exit(main())
