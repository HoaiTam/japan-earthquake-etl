#!/usr/bin/env python3
"""Package and verify the 47 requested original metadata objects for DAT-03 follow-up (rosy179).

Fulfills config/research/dat03_lineage_rosy179_followup_20261114.json:
- 23 gold_identity objects (22 parent + 1 head)
- 1 gold_commit object (head: orc07-22b5e7647a3df9ed83370139e11c48048845ee8f)
- 23 gold_publication objects
Total: 47 original objects + 1 manifest.json = 48 entries.
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
from typing import Any, Dict, List, Optional, Tuple
import zipfile

try:
    import boto3
except ImportError:
    boto3 = None

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REQUEST_PATH = ROOT / "config/research/dat03_lineage_rosy179_followup_20261114.json"
DEFAULT_OUTPUT_ZIP = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-dat03-lineage-followup.zip"
DEFAULT_RAW_DIR = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-dat03-followup-raw"


def extract_identity_payload_digest(data: bytes) -> Tuple[str, str]:
    """Extract internal identity_sha256 and recompute payload digest per Java contract.

    Java GoldIcebergWriter logic:
        String identitySha = SilverBundlePublisher.sha(JSON.writeValueAsBytes(identity));
        identity.put("identity_sha256", identitySha);
        byte[] claimBytes = JSON.writeValueAsBytes(identity);
    """
    parsed = json.loads(data.decode("utf-8"))
    internal_sha = parsed.get("identity_sha256")
    if not internal_sha:
        raise ValueError("MISSING_IDENTITY_SHA256: identity.json lacks identity_sha256 field")
    needle = b',"identity_sha256":"' + internal_sha.encode("ascii") + b'"}'
    if not data.endswith(needle):
        raise ValueError("INVALID_SERIALIZATION_SUFFIX: identity.json does not end with identity_sha256 suffix")
    payload_bytes = data[:-len(needle)] + b"}"
    recomputed = hashlib.sha256(payload_bytes).hexdigest()
    return internal_sha, recomputed


def get_s3_client(endpoint_url: Optional[str] = None):
    if boto3 is None:
        raise RuntimeError("boto3 is required to interact with MinIO/S3 when source-dir is not provided.")
    endpoint = (
        endpoint_url
        or os.environ.get("MINIO_ENDPOINT")
        or os.environ.get("MINIO_ENDPOINT_URL")
        or "http://localhost:9000"
    )
    access_key = os.environ.get("MINIO_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    region = os.environ.get("S3_REGION", "us-east-1")

    client_kwargs: Dict[str, Any] = {
        "endpoint_url": endpoint,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": region,
    }
    if session_token:
        client_kwargs["aws_session_token"] = session_token
    return boto3.client("s3", **client_kwargs)


def fetch_bytes(
    key: str,
    bucket: str = "japan-earthquake",
    source_dir: Optional[Path] = None,
    s3_client: Any = None,
) -> bytes:
    """Fetch object bytes from local source directory or MinIO S3."""
    if source_dir:
        src_path = (source_dir / key).resolve()
        if src_path.is_file() and not src_path.is_symlink():
            return src_path.read_bytes()

    if s3_client is not None:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    raise FileNotFoundError(f"SOURCE_OBJECT_NOT_FOUND: could not resolve key {key} in {source_dir} or via S3")


def pack_followup_archive(
    request_file: Path = DEFAULT_REQUEST_PATH,
    output_zip: Path = DEFAULT_OUTPUT_ZIP,
    source_dir: Optional[Path] = DEFAULT_RAW_DIR,
    endpoint_url: Optional[str] = None,
    bucket: str = "japan-earthquake",
    overwrite: bool = True,
) -> Dict[str, Any]:
    """Package 47 original metadata objects into a verifiable ZIP archive."""
    req_path = Path(request_file).resolve()
    if not req_path.is_file():
        raise FileNotFoundError(f"REQUEST_FILE_NOT_FOUND: {req_path}")

    req_data = json.loads(req_path.read_text(encoding="utf-8"))
    owner = req_data.get("owner", "rosy179")
    if owner != "rosy179":
        raise ValueError(f"UNEXPECTED_OWNER: expected rosy179, got {owner}")

    pinned_objects = req_data.get("objects", [])
    if len(pinned_objects) != 24:
        raise ValueError(f"UNEXPECTED_PINNED_OBJECTS_COUNT: expected 24, got {len(pinned_objects)}")

    pub_uris = req_data.get("unpinned_publication_uris", [])
    if len(pub_uris) != 23:
        raise ValueError(f"UNEXPECTED_PUBLICATION_URIS_COUNT: expected 23, got {len(pub_uris)}")

    out_zip = Path(output_zip).resolve()
    out_dir = out_zip.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    sha_path = out_dir / f"{out_zip.name}.sha256"
    if (out_zip.exists() or sha_path.exists()) and not overwrite:
        raise FileExistsError(f"OUTPUT_ALREADY_EXISTS: {out_zip} (pass overwrite=True)")

    # Prepare S3 client if source_dir is not fully available
    src_dir_resolved: Optional[Path] = Path(source_dir).resolve() if source_dir and Path(source_dir).exists() else None
    s3 = None
    if not src_dir_resolved:
        s3 = get_s3_client(endpoint_url=endpoint_url)

    manifest_entries: List[Dict[str, Any]] = []
    files_to_pack: List[Tuple[str, bytes]] = []
    total_bytes = 0

    print("Fetching and verifying 24 pinned objects...")
    for item in pinned_objects:
        uri = item["uri"]
        rel_key = uri.replace(f"s3://{bucket}/", "")
        data = fetch_bytes(rel_key, bucket=bucket, source_dir=src_dir_resolved, s3_client=s3)
        total_bytes += len(data)
        raw_sha = hashlib.sha256(data).hexdigest()
        expected = item["sha256"]
        kind = item.get("kind")

        entry: Dict[str, Any] = {
            "uri": uri,
            "relative_path": rel_key,
            "kind": kind,
            "size": len(data),
            "sha256": raw_sha,
            "expected_sha256": expected,
        }

        if kind == "gold_identity":
            internal_sha, recomputed_sha = extract_identity_payload_digest(data)
            if recomputed_sha != expected:
                raise ValueError(f"IDENTITY_PAYLOAD_MISMATCH: {rel_key} got {recomputed_sha}, expected {expected}")
            if internal_sha != expected:
                raise ValueError(f"IDENTITY_INTERNAL_MISMATCH: {rel_key} got {internal_sha}, expected {expected}")
            entry["identity_payload_sha256"] = recomputed_sha
            entry["internal_identity_sha256"] = internal_sha
            entry["status"] = "MATCHED"
        elif kind == "gold_commit":
            if raw_sha != expected:
                raise ValueError(f"COMMIT_SHA_MISMATCH: {rel_key} got {raw_sha}, expected {expected}")
            entry["status"] = "MATCHED"
        else:
            raise ValueError(f"UNKNOWN_KIND: {kind} for {uri}")

        manifest_entries.append(entry)
        files_to_pack.append((rel_key, data))

    print("Fetching and verifying 23 publication objects...")
    for uri in pub_uris:
        rel_key = uri.replace(f"s3://{bucket}/", "")
        data = fetch_bytes(rel_key, bucket=bucket, source_dir=src_dir_resolved, s3_client=s3)
        total_bytes += len(data)
        raw_sha = hashlib.sha256(data).hexdigest()
        pub_json = json.loads(data.decode("utf-8"))

        entry = {
            "uri": uri,
            "relative_path": rel_key,
            "kind": "gold_publication",
            "size": len(data),
            "sha256": raw_sha,
            "publication_id": pub_json.get("publication_id"),
            "verify_status": pub_json.get("verify_status"),
            "published": pub_json.get("published"),
            "event_count": pub_json.get("event_count"),
            "verified_at_utc": pub_json.get("verified_at_utc"),
            "identity_sha256": pub_json.get("identity_sha256"),
            "bundle_sha256": pub_json.get("bundle_sha256"),
            "status": "FETCHED_ORIGINAL",
        }
        manifest_entries.append(entry)
        files_to_pack.append((rel_key, data))

    # Manifest file
    package_manifest = {
        "owner": owner,
        "source": f"MinIO original objects (bucket: {bucket})",
        "total_objects": len(manifest_entries),
        "total_bytes": total_bytes,
        "objects": manifest_entries,
    }
    manifest_bytes = json.dumps(package_manifest, indent=2, ensure_ascii=False).encode("utf-8")
    files_to_pack.append(("manifest.json", manifest_bytes))

    # Deterministic atomic ZIP write
    pid = os.getpid()
    tmp_zip = out_dir / f"{out_zip.name}.tmp.{pid}"
    tmp_sha = out_dir / f"{sha_path.name}.tmp.{pid}"
    fixed_time = (2026, 11, 14, 21, 4, 0)

    try:
        with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for arc_path, content in files_to_pack:
                zinfo = zipfile.ZipInfo(arc_path)
                zinfo.date_time = fixed_time
                zinfo.compress_type = zipfile.ZIP_DEFLATED
                zinfo.external_attr = 0o644 << 16
                zf.writestr(zinfo, content)

        # Integrity test
        with zipfile.ZipFile(tmp_zip, "r") as zf:
            assert zf.testzip() is None, "CORRUPT_ZIP_OUTPUT"

        zip_bytes = tmp_zip.read_bytes()
        zip_sha = hashlib.sha256(zip_bytes).hexdigest()
        tmp_sha.write_text(f"{zip_sha}  {out_zip.name}\n", encoding="utf-8")

        os.replace(tmp_zip, out_zip)
        os.replace(tmp_sha, sha_path)
    finally:
        if tmp_zip.exists():
            tmp_zip.unlink(missing_ok=True)
        if tmp_sha.exists():
            tmp_sha.unlink(missing_ok=True)

    print(f"Followup package created: {out_zip} ({len(zip_bytes)} bytes, SHA: {zip_sha})")
    return {
        "status": "SUCCESS",
        "archive": str(out_zip),
        "bytes": len(zip_bytes),
        "sha256": zip_sha,
        "object_count": len(manifest_entries),
    }


def verify_followup_archive(
    zip_path: Path = DEFAULT_OUTPUT_ZIP,
    request_file: Path = DEFAULT_REQUEST_PATH,
) -> Dict[str, Any]:
    """Verify integrity and cryptographic compliance of the followup ZIP archive."""
    zp = Path(zip_path).resolve()
    if not zp.is_file():
        raise FileNotFoundError(f"ZIP_NOT_FOUND: {zp}")

    req_p = Path(request_file).resolve()
    if not req_p.is_file():
        raise FileNotFoundError(f"REQUEST_NOT_FOUND: {req_p}")

    req_data = json.loads(req_p.read_text(encoding="utf-8"))
    pinned_by_uri = {item["uri"]: item for item in req_data.get("objects", [])}
    expected_pub_uris = set(req_data.get("unpinned_publication_uris", []))

    with zipfile.ZipFile(zp, "r") as zf:
        if zf.testzip() is not None:
            raise ValueError(f"CORRUPT_ZIP: testzip failed on {zp}")

        names = zf.namelist()
        if len(names) != 48:
            raise ValueError(f"UNEXPECTED_MEMBER_COUNT: expected 48, got {len(names)}")

        if "manifest.json" not in names:
            raise ValueError("MISSING_MANIFEST: manifest.json not found in ZIP")

        manifest_data = json.loads(zf.read("manifest.json").decode("utf-8"))
        if manifest_data.get("owner") != "rosy179":
            raise ValueError(f"INVALID_MANIFEST_OWNER: {manifest_data.get('owner')}")

        matched_pinned = 0
        matched_pubs = 0

        for name in names:
            if name == "manifest.json":
                continue

            content = zf.read(name)
            uri = f"s3://japan-earthquake/{name}"

            if uri in pinned_by_uri:
                item = pinned_by_uri[uri]
                expected_sha = item["sha256"]
                kind = item["kind"]

                if kind == "gold_identity":
                    internal_sha, recomputed = extract_identity_payload_digest(content)
                    if recomputed != expected_sha or internal_sha != expected_sha:
                        raise ValueError(f"IDENTITY_MISMATCH inside ZIP for {name}: {recomputed} != {expected_sha}")
                elif kind == "gold_commit":
                    raw_sha = hashlib.sha256(content).hexdigest()
                    if raw_sha != expected_sha:
                        raise ValueError(f"COMMIT_MISMATCH inside ZIP for {name}: {raw_sha} != {expected_sha}")
                matched_pinned += 1

            elif uri in expected_pub_uris:
                pub_obj = json.loads(content.decode("utf-8"))
                if pub_obj.get("verify_status") != "PASSED" or pub_obj.get("published") is not True:
                    raise ValueError(f"UNPUBLISHED_PUBLICATION inside ZIP for {name}")
                matched_pubs += 1
            else:
                raise ValueError(f"UNEXPECTED_ZIP_MEMBER: {name}")

        if matched_pinned != 24:
            raise ValueError(f"PINNED_MATCH_COUNT_MISMATCH: expected 24, got {matched_pinned}")
        if matched_pubs != 23:
            raise ValueError(f"PUB_MATCH_COUNT_MISMATCH: expected 23, got {matched_pubs}")

    zip_bytes = zp.read_bytes()
    zip_sha = hashlib.sha256(zip_bytes).hexdigest()
    return {
        "status": "VERIFIED",
        "archive": str(zp),
        "bytes": len(zip_bytes),
        "sha256": zip_sha,
        "pinned_matched": matched_pinned,
        "pubs_matched": matched_pubs,
        "total_entries": len(names),
    }


def parse_args():
    parser = argparse.ArgumentParser(description="Package and verify DAT-03 followup metadata for rosy179")
    parser.add_argument("--pack", action="store_true", help="Package raw objects into ZIP archive")
    parser.add_argument("--verify", action="store_true", help="Verify existing ZIP archive")
    parser.add_argument("--output-zip", type=str, default=str(DEFAULT_OUTPUT_ZIP), help="Output ZIP path")
    parser.add_argument("--source-dir", type=str, default=str(DEFAULT_RAW_DIR), help="Source directory containing raw objects")
    parser.add_argument("--request-file", type=str, default=str(DEFAULT_REQUEST_PATH), help="Followup request JSON")
    parser.add_argument("--bucket", type=str, default="japan-earthquake", help="S3 bucket")
    parser.add_argument("--endpoint-url", type=str, default=None, help="MinIO S3 endpoint URL")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwrite of output archive")
    return parser.parse_args()


def main():
    args = parse_args()
    req_file = Path(args.request_file)
    zip_file = Path(args.output_zip)
    src_dir = Path(args.source_dir) if args.source_dir else None

    if args.verify:
        result = verify_followup_archive(zip_path=zip_file, request_file=req_file)
        print(f"VERIFICATION SUCCESSFUL: {result}")
        return 0

    # Default action is pack followed by verify
    pack_res = pack_followup_archive(
        request_file=req_file,
        output_zip=zip_file,
        source_dir=src_dir,
        endpoint_url=args.endpoint_url,
        bucket=args.bucket,
        overwrite=args.overwrite or True,
    )
    verify_res = verify_followup_archive(zip_path=zip_file, request_file=req_file)
    print(f"ALL CHECKS PASSED: {verify_res}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
