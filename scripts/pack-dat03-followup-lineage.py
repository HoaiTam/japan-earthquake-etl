#!/usr/bin/env python3
"""Package the 3 requested original metadata objects for DAT-03 follow-up."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import zipfile

try:
    import boto3
except ImportError:
    boto3 = None

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "orc07-5679c9c378edacb4e5f804613c8475351ce326b4"

EXPECTED_COMMIT_SHA = "cfbc547badf4807c2530d2960a0cb3988d21a82913f299113787e4dc61da5e72"
EXPECTED_IDENTITY_PAYLOAD_SHA = "140c5ba2919d1c519faaecc25ddaaae6d7e94b1c7f564cf63613e3824572b3fd"

OBJECTS_CONFIG = [
    {
        "key": f"silver/gold-commits/{RUN_ID}/commit.json",
        "kind": "gold_commit",
        "expected_file_sha": EXPECTED_COMMIT_SHA,
    },
    {
        "key": f"silver/gold-commits/{RUN_ID}/identity.json",
        "kind": "gold_identity",
        "expected_payload_sha": EXPECTED_IDENTITY_PAYLOAD_SHA,
    },
    {
        "key": f"silver/gold-verification/{RUN_ID}/publication.json",
        "kind": "gold_publication",
    },
]


def extract_identity_payload_digest(data: bytes) -> tuple[str, str]:
    """Extract internal identity_sha256 and recomputed payload digest per Java contract."""
    parsed = json.loads(data.decode("utf-8"))
    internal_sha = parsed.get("identity_sha256")
    if not internal_sha:
        raise ValueError("Missing identity_sha256 field in identity.json")
    needle = b',"identity_sha256":"' + internal_sha.encode("ascii") + b'"}'
    if not data.endswith(needle):
        raise ValueError("Invalid serialization suffix in identity.json")
    payload_bytes = data[:-len(needle)] + b'}'
    recomputed = hashlib.sha256(payload_bytes).hexdigest()
    return internal_sha, recomputed


def fetch_from_minio(bucket: str, key: str) -> bytes:
    if boto3 is None:
        raise RuntimeError("boto3 is required")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
    )
    resp = s3.get_object(Bucket=bucket, Key=key)
    return resp["Body"].read()


def main() -> int:
    bucket = os.environ.get("DATA_BUCKET", "japan-earthquake")
    out_dir = ROOT / "staging/dat03-lineage"
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / "ThanhTris-dat03-lineage-followup.zip"
    sha_path = out_dir / "ThanhTris-dat03-lineage-followup.zip.sha256"

    manifest_entries = []
    files_to_zip: list[tuple[str, bytes]] = []

    print("Fetching and verifying objects from MinIO...")
    for item in OBJECTS_CONFIG:
        key = item["key"]
        uri = f"s3://{bucket}/{key}"
        data = fetch_from_minio(bucket, key)
        raw_sha = hashlib.sha256(data).hexdigest()
        byte_size = len(data)

        entry = {
            "uri": uri,
            "archive_path": key,
            "kind": item["kind"],
            "byte_size": byte_size,
            "raw_sha256": raw_sha,
        }

        if item["kind"] == "gold_commit":
            expected = item["expected_file_sha"]
            if raw_sha != expected:
                raise ValueError(f"COMMIT_SHA_MISMATCH: got {raw_sha}, expected {expected}")
            entry["expected_raw_sha256"] = expected
            entry["status"] = "VERIFIED_MATCH"
            print(f"  [OK] commit.json: SHA256={raw_sha}")

        elif item["kind"] == "gold_identity":
            internal_sha, recomputed_sha = extract_identity_payload_digest(data)
            expected = item["expected_payload_sha"]
            if recomputed_sha != expected:
                raise ValueError(f"IDENTITY_PAYLOAD_SHA_MISMATCH: got {recomputed_sha}, expected {expected}")
            if internal_sha != expected:
                raise ValueError(f"IDENTITY_INTERNAL_SHA_MISMATCH: got {internal_sha}, expected {expected}")
            entry["identity_payload_sha256"] = recomputed_sha
            entry["internal_identity_sha256"] = internal_sha
            entry["expected_payload_sha256"] = expected
            entry["status"] = "VERIFIED_MATCH"
            print(f"  [OK] identity.json: Raw-SHA256={raw_sha}, Payload-Digest={recomputed_sha}")

        elif item["kind"] == "gold_publication":
            parsed = json.loads(data.decode("utf-8"))
            verify_status = parsed.get("verify_status")
            published = parsed.get("published")
            entry["verify_status"] = verify_status
            entry["published"] = published
            entry["status"] = "ORIGINAL_RETRIEVED"
            print(f"  [OK] publication.json: Raw-SHA256={raw_sha}, verify_status={verify_status}, published={published}")

        files_to_zip.append((key, data))
        manifest_entries.append(entry)

    manifest_data = {
        "version": "dat03-lineage-followup-manifest-v1",
        "owner": "ThanhTris",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": RUN_ID,
        "object_count": len(manifest_entries),
        "objects": manifest_entries,
    }

    manifest_bytes = json.dumps(manifest_data, indent=2).encode("utf-8")
    files_to_zip.append(("manifest.json", manifest_bytes))

    # Build ZIP archive with deterministic attributes
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for arc_path, content in files_to_zip:
            zinfo = zipfile.ZipInfo(arc_path)
            zinfo.date_time = (2026, 11, 14, 21, 30, 0)
            zinfo.compress_type = zipfile.ZIP_DEFLATED
            zinfo.external_attr = 0o644 << 16
            zf.writestr(zinfo, content)

    zip_bytes = zip_path.read_bytes()
    zip_sha = hashlib.sha256(zip_bytes).hexdigest()
    zip_size = len(zip_bytes)

    sha_content = f"{zip_sha}  {zip_path.name}\n"
    sha_path.write_text(sha_content, encoding="utf-8")

    print("\nPackage created successfully:")
    print(f"  Archive: {zip_path}")
    print(f"  Size:    {zip_size} bytes")
    print(f"  SHA-256: {zip_sha}")
    print(f"  Checksum file: {sha_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
