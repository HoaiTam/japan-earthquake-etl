#!/usr/bin/env python3
"""Extract first-hop provenance objects from MinIO for worker nodes (DAT-03).

Downloads required first-hop objects (Bronze manifests, Silver bundle manifests,
snapshot registry records) directly from MinIO / S3, validates SHA-256 against
trusted requirement mapping, and packages them into a verifiable provenance ZIP.

Fail-Closed Policy:
- Output immutability: fails if output ZIP exists unless --overwrite is explicitly set.
- Checksum verification: verifies SHA-256 for each object upon retrieval; fails on mismatch.
- Path safety: validates all object keys against directory traversal, symlinks, or absolute paths.
- Pinned checkpoint compatibility: supports exact schema {"objects": [{"uri": ..., "sha256": ..., "kind": ...}]}.
- Configuration: reads S3 credentials strictly via environment variables or CLI options.
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

try:
    import boto3
except ImportError:
    boto3 = None

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


def check_raw_path_safety(raw_path: str) -> Tuple[bool, str]:
    """Validate raw path string strictly before any stripping or modification.

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
    """Parse raw URI or relative path into (s3_uri, rel_key) with fail-closed safety.

    Validates path safety BEFORE any stripping.
    """
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


def resolve_category(key: str, kind: Optional[str] = None) -> str:
    """Normalize object category from kind or path."""
    k = (kind or "").strip().lower()
    if k in ("bronze_manifest", "bronze"):
        return "bronze_manifest"
    if k in ("silver_bundle_manifest", "silver_bundle", "silver"):
        return "silver_bundle_manifest"
    if k in ("registry_json", "registry", "snapshot_registry"):
        return "registry_json"

    # Derive from key path
    if key.startswith("bronze/"):
        return "bronze_manifest"
    elif "bundles/" in key or key.startswith("silver/bundles/"):
        return "silver_bundle_manifest"
    elif "full-month/" in key or key.startswith("silver/full-month/") or "registry" in key:
        return "registry_json"
    return "unknown"


def load_expected_mapping(source_path: Path, bucket: str = "japan-earthquake") -> List[Dict[str, Any]]:
    """Load and parse expected object mapping from file.

    Supports:
    - Pinned checkpoint schema: {"objects": [{"uri": ..., "sha256": ..., "kind": ...}]}
    - Required provenance schema: {"checksums": {path: sha256}}
    - Package manifest schema: {"objects": [{"path"|"archive_path"|"s3_uri": ..., "sha256": ...}]}
    - Flat dict: {path_or_uri: sha256}
    - List of objects: [{"uri"|"path"|"s3_uri": ..., "sha256": ...}]
    """
    source_path = Path(source_path).resolve()
    if not source_path.is_file():
        raise FileNotFoundError(f"SOURCE_MAPPING_FILE_NOT_FOUND: {source_path}")

    with open(source_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_items: List[Tuple[str, str, Optional[str]]] = []

    if isinstance(data, dict):
        if "checksums" in data and isinstance(data["checksums"], dict):
            for k, v in data["checksums"].items():
                raw_items.append((k, str(v).strip().lower(), None))
        elif "objects" in data and isinstance(data["objects"], list):
            for item in data["objects"]:
                # Support uri, s3_uri, path, archive_path
                p = item.get("uri") or item.get("s3_uri") or item.get("path") or item.get("archive_path")
                sha = item.get("sha256") or item.get("sha") or item.get("checksum")
                kind = item.get("kind") or item.get("category")
                if p and sha:
                    raw_items.append((str(p), str(sha).strip().lower(), str(kind) if kind else None))
        elif "required_objects" in data and isinstance(data["required_objects"], list):
            for item in data["required_objects"]:
                p = item.get("uri") or item.get("s3_uri") or item.get("path") or item.get("archive_path")
                sha = item.get("sha256") or item.get("sha") or item.get("checksum")
                kind = item.get("kind") or item.get("category")
                if p and sha:
                    raw_items.append((str(p), str(sha).strip().lower(), str(kind) if kind else None))
        else:
            # Flat dict
            for k, v in data.items():
                if isinstance(v, str) and re.fullmatch(r"[0-9a-fA-F]{64}", v.strip()):
                    raw_items.append((k, v.strip().lower(), None))

    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                p = item.get("uri") or item.get("s3_uri") or item.get("path") or item.get("archive_path")
                sha = item.get("sha256") or item.get("sha") or item.get("checksum")
                kind = item.get("kind") or item.get("category")
                if p and sha:
                    raw_items.append((str(p), str(sha).strip().lower(), str(kind) if kind else None))

    if not raw_items:
        raise ValueError(f"EMPTY_OR_UNRECOGNIZED_SOURCE_MAPPING: {source_path}")

    items: List[Dict[str, Any]] = []
    seen_keys: Set[str] = set()

    for raw_p, exp_sha, raw_kind in sorted(raw_items, key=lambda x: x[0]):
        s3_uri, key = parse_uri_or_key(raw_p, default_bucket=bucket)
        if key in seen_keys:
            raise ValueError(f"DUPLICATE_KEY_IN_MAPPING: {key}")
        seen_keys.add(key)
        cat = resolve_category(key, kind=raw_kind)
        items.append({
            "category": cat,
            "kind": cat,
            "uri": s3_uri,
            "s3_uri": s3_uri,
            "archive_path": key,
            "path": key,
            "expected_sha256": exp_sha,
        })

    return items


def extract_and_pack_provenance(
    owner: str,
    source_mapping_path: Path,
    output_zip: Path,
    output_manifest: Optional[Path] = None,
    temp_dir: Optional[Path] = None,
    overwrite: bool = False,
    s3_client: Optional[Any] = None,
    bucket: str = "japan-earthquake",
    endpoint_url: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """Extract required provenance objects from MinIO, verify hashes, and package ZIP."""
    if owner not in WORKER_PROVENANCE_SPECS:
        raise ValueError(f"UNRECOGNIZED_OWNER: {owner}")

    spec = WORKER_PROVENANCE_SPECS[owner]
    required_count = spec["total_required_objects"]

    output_zip = Path(output_zip).resolve()
    if output_zip.exists() and not overwrite:
        raise FileExistsError(f"OUTPUT_FILE_ALREADY_EXISTS: {output_zip}. Set overwrite=True to allow replacement.")

    if output_manifest:
        output_manifest = Path(output_manifest).resolve()
        if output_manifest.exists() and not overwrite:
            raise FileExistsError(f"OUTPUT_FILE_ALREADY_EXISTS: {output_manifest}. Set overwrite=True to allow replacement.")

    expected_items = load_expected_mapping(source_mapping_path, bucket=bucket)
    if len(expected_items) != required_count:
        raise ValueError(
            f"OBJECT_COUNT_MISMATCH: expected {required_count} objects for {owner}, mapping contains {len(expected_items)}"
        )

    # Validate category breakdown
    b_cnt = sum(1 for x in expected_items if x["category"] == "bronze_manifest")
    s_cnt = sum(1 for x in expected_items if x["category"] == "silver_bundle_manifest")
    r_cnt = sum(1 for x in expected_items if x["category"] == "registry_json")
    if (
        b_cnt != spec["bronze_manifest_count"]
        or s_cnt != spec["silver_manifest_count"]
        or r_cnt != spec["registry_json_count"]
    ):
        raise ValueError(
            f"CATEGORY_BREAKDOWN_MISMATCH: expected {spec['bronze_manifest_count']}/{spec['silver_manifest_count']}/{spec['registry_json_count']}, "
            f"got {b_cnt}/{s_cnt}/{r_cnt}"
        )

    # Initialize S3 client if not passed
    if s3_client is None:
        if boto3 is None:
            raise RuntimeError("boto3 is required when s3_client is not provided. Install boto3 or run inside Airflow/Docker container.")
        endpoint = endpoint_url or os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
        ak = access_key or os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID", "pipeline-access-key")
        sk = secret_key or os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY", "pipeline-secret-key")
        reg = region or os.environ.get("S3_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=ak,
            aws_secret_access_key=sk,
            region_name=reg,
        )

    # 1. Resolve unique, isolated staging directory
    if temp_dir:
        staging_dir = Path(temp_dir).resolve()
        if staging_dir.exists() and not overwrite:
            existing_files = [p for p in staging_dir.rglob("*") if p.is_file() or p.is_symlink() or os.path.islink(p)]
            if existing_files:
                raise FileExistsError(
                    f"STAGING_DIR_NOT_EMPTY: {staging_dir} contains {len(existing_files)} existing files. "
                    "Refusing to reuse staging without overwrite=True."
                )
        staging_dir.mkdir(parents=True, exist_ok=True)
    else:
        import uuid
        run_id = f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
        staging_dir = (output_zip.parent / f".tmp_{owner}_{run_id}").resolve()
        staging_dir.mkdir(parents=True, exist_ok=False)

    abs_staging = staging_dir.resolve()
    manifest_items: List[Dict[str, Any]] = []

    for item in expected_items:
        key = item["archive_path"]
        expected_sha = item["expected_sha256"]
        target_p = staging_dir / key

        # 2. Path traversal boundary check: target must remain strictly inside staging_dir
        try:
            target_p.resolve().relative_to(abs_staging)
        except ValueError:
            raise ValueError(f"PATH_OUTSIDE_STAGING: {target_p} escapes {abs_staging}")

        # 3. Symlink guard: check target and all parent components between target and staging
        curr = target_p
        while curr != staging_dir and curr != curr.parent:
            if curr.is_symlink() or os.path.islink(curr):
                raise ValueError(f"SYMLINK_IN_STAGING_FORBIDDEN: {curr} is a symlink")
            curr = curr.parent

        if target_p.is_symlink() or os.path.islink(target_p):
            raise ValueError(f"SYMLINK_IN_STAGING_FORBIDDEN: {target_p} is a symlink")

        # 4. Immutability guard: refuse to overwrite existing staging file unless explicit
        if target_p.exists() and not overwrite:
            raise FileExistsError(
                f"STAGING_FILE_ALREADY_EXISTS: {target_p}. Set overwrite=True to allow replacement."
            )

        # 5. Fetch from S3
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()

        actual_sha = hashlib.sha256(body).hexdigest()
        if actual_sha != expected_sha:
            raise ValueError(
                f"SHA_MISMATCH for key {key}: expected {expected_sha}, retrieved {actual_sha}"
            )

        # Save to local staging safely
        target_p.parent.mkdir(parents=True, exist_ok=True)
        target_p.write_bytes(body)

        manifest_items.append({
            "category": item["category"],
            "kind": item["kind"],
            "uri": item["uri"],
            "s3_uri": item["s3_uri"],
            "archive_path": key,
            "path": key,
            "bytes": len(body),
            "sha256": actual_sha,
        })

    # Prepare package manifest
    package_manifest = {
        "version": "dat03-provenance-package-v1",
        "owner": owner,
        "range": f"{spec['range'][0]}..{spec['range'][1]}",
        "month_count": spec["month_count"],
        "total_objects": len(manifest_items),
        "required_objects": required_count,
        "counts_by_category": {
            "bronze_manifest": b_cnt,
            "silver_bundle_manifest": s_cnt,
            "registry_json": r_cnt,
        },
        "packaged_at_utc": datetime.now(timezone.utc).isoformat(),
        "objects": manifest_items,
    }

    manifest_bytes = json.dumps(package_manifest, indent=2).encode("utf-8")
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

    output_zip.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", manifest_bytes)
        for item in manifest_items:
            fpath = staging_dir / item["archive_path"]
            zf.write(fpath, arcname=item["archive_path"])

    zip_bytes = output_zip.read_bytes()
    zip_sha = hashlib.sha256(zip_bytes).hexdigest()

    # Write required-provenance metadata JSON if requested
    if output_manifest:
        output_manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest_meta = {
            "owner": owner,
            "total_objects": len(manifest_items),
            "zip_sha256": zip_sha,
            "manifest_sha256": manifest_sha,
            "counts": package_manifest["counts_by_category"],
            "objects": [
                {
                    "uri": item["uri"],
                    "s3_uri": item["s3_uri"],
                    "archive_path": item["archive_path"],
                    "path": item["path"],
                    "kind": item["kind"],
                    "category": item["category"],
                    "bytes": item["bytes"],
                    "sha256": item["sha256"],
                }
                for item in manifest_items
            ],
            "checksums": {item["archive_path"]: item["sha256"] for item in manifest_items},
        }
        output_manifest.write_text(json.dumps(manifest_meta, indent=2), encoding="utf-8")

    return {
        "status": "PASS",
        "owner": owner,
        "output_zip": str(output_zip),
        "zip_sha256": zip_sha,
        "manifest_sha256": manifest_sha,
        "total_objects": len(manifest_items),
        "required_objects": required_count,
        "counts_by_category": package_manifest["counts_by_category"],
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract first-hop provenance objects from MinIO for worker nodes."
    )
    parser.add_argument("--owner", default="rosy179", choices=["rosy179", "ThanhTris"], help="Worker owner.")
    parser.add_argument("--source-mapping", required=True, type=Path, help="Path to trusted requirements JSON.")
    parser.add_argument("--output-zip", required=True, type=Path, help="Path for output ZIP archive.")
    parser.add_argument("--output-manifest", type=Path, help="Optional path to output required-provenance JSON.")
    parser.add_argument("--temp-dir", type=Path, help="Optional staging directory for raw objects.")
    parser.add_argument("--overwrite", action="store_true", default=False, help="Allow overwriting existing outputs.")
    parser.add_argument("--bucket", default="japan-earthquake", help="S3 bucket name.")
    parser.add_argument("--endpoint-url", help="Override MinIO endpoint URL.")
    parser.add_argument("--access-key", help="Override MinIO access key.")
    parser.add_argument("--secret-key", help="Override MinIO secret key.")
    parser.add_argument("--region", help="Override S3 region.")

    args = parser.parse_args(argv)

    try:
        res = extract_and_pack_provenance(
            owner=args.owner,
            source_mapping_path=args.source_mapping,
            output_zip=args.output_zip,
            output_manifest=args.output_manifest,
            temp_dir=args.temp_dir,
            overwrite=args.overwrite,
            bucket=args.bucket,
            endpoint_url=args.endpoint_url,
            access_key=args.access_key,
            secret_key=args.secret_key,
            region=args.region,
        )
        print(json.dumps(res, indent=2))
        return 0
    except Exception as ex:
        print(json.dumps({"status": "FAIL", "reason": str(ex)}, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
