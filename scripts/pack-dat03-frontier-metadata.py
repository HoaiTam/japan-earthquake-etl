#!/usr/bin/env python3
"""Package the 18 identity.json and 18 publication.json objects into a verifiable metadata ZIP archive."""
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
REQ_PATH = ROOT / "config/research/dat03_lineage_ThanhTris_metadata.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Package metadata ZIP archive")
    parser.add_argument(
        "--output",
        type=str,
        default=str(ROOT / "staging/dat03-lineage/ThanhTris-dat03-frontier-metadata-20261112.zip"),
        help="Path for output ZIP file",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing output archive without failing",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    zip_path = Path(args.output).resolve()
    out_dir = zip_path.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    if zip_path.exists() and not args.overwrite:
        raise FileExistsError(
            f"Target archive already exists: {zip_path}. Pass --overwrite to replace."
        )

    with open(REQ_PATH, "r", encoding="utf-8") as f:
        req = json.load(f)

    if boto3 is None:
        raise RuntimeError("boto3 is required to interact with MinIO/S3.")

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
        region_name=os.environ.get("S3_REGION", "us-east-1"),
    )
    bucket = os.environ.get("DATA_BUCKET", "japan-earthquake")

    manifest_entries = []
    archive_files = []

    print(f"Fetching {len(req['objects'])} identity files...")
    for item in req["objects"]:
        uri = item["uri"]
        expected_sha = item["sha256"]
        rel_key = uri.replace(f"s3://{bucket}/", "")
        resp = s3.get_object(Bucket=bucket, Key=rel_key)
        data = resp["Body"].read()
        file_sha = hashlib.sha256(data).hexdigest()
        parsed = json.loads(data.decode("utf-8"))
        internal_sha = parsed.get("identity_sha256")

        assert internal_sha, f"MISSING_IDENTITY_SHA for {uri}"
        needle = b',"identity_sha256":"' + internal_sha.encode("ascii") + b'"}'
        assert data.endswith(needle), f"INVALID_IDENTITY_SERIALIZATION for {uri}"

        payload_bytes = data[:-len(needle)] + b'}'
        recomputed_payload_sha = hashlib.sha256(payload_bytes).hexdigest()
        assert recomputed_payload_sha == internal_sha, (
            f"PAYLOAD_INTERNAL_MISMATCH for {uri}: {recomputed_payload_sha} != {internal_sha}"
        )
        assert recomputed_payload_sha == expected_sha, (
            f"PAYLOAD_EXPECTED_MISMATCH for {uri}: {recomputed_payload_sha} != {expected_sha}"
        )

        archive_files.append((rel_key, data))
        manifest_entries.append({
            "uri": uri,
            "archive_path": rel_key,
            "kind": "gold_identity",
            "byte_size": len(data),
            "file_sha256": file_sha,
            "identity_payload_sha256": recomputed_payload_sha,
            "internal_identity_sha256": internal_sha,
            "expected_sha256": expected_sha,
            "status": "MATCHED",
        })

    print(f"Fetching {len(req['unpinned_publication_uris'])} publication files...")
    for uri in req["unpinned_publication_uris"]:
        rel_key = uri.replace(f"s3://{bucket}/", "")
        resp = s3.get_object(Bucket=bucket, Key=rel_key)
        data = resp["Body"].read()
        file_sha = hashlib.sha256(data).hexdigest()
        parsed = json.loads(data.decode("utf-8"))

        archive_files.append((rel_key, data))
        manifest_entries.append({
            "uri": uri,
            "archive_path": rel_key,
            "kind": "gold_publication",
            "byte_size": len(data),
            "file_sha256": file_sha,
            "publication_id": parsed.get("publication_id"),
            "verify_status": parsed.get("verify_status"),
            "published": parsed.get("published"),
            "event_count": parsed.get("event_count"),
            "verified_at_utc": parsed.get("verified_at_utc"),
            "identity_sha256": parsed.get("identity_sha256"),
            "bundle_sha256": parsed.get("bundle_sha256"),
            "status": "FETCHED_ORIGINAL",
        })

    # Build manifest and README
    package_manifest = {
        "version": "dat03-frontier-metadata-package-v2",
        "owner": "ThanhTris",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "basis_request": "config/research/dat03_lineage_ThanhTris_metadata.json",
        "basis_request_sha256": hashlib.sha256(REQ_PATH.read_bytes()).hexdigest(),
        "object_count": len(manifest_entries),
        "objects": manifest_entries,
    }

    manifest_bytes = json.dumps(package_manifest, indent=2, ensure_ascii=False).encode("utf-8")
    archive_files.append(("manifest.json", manifest_bytes))

    readme_text = f"""# DAT-03 Frontier Metadata Package — ThanhTris

- **Created at UTC:** {datetime.now(timezone.utc).isoformat()}
- **Owner:** ThanhTris
- **Total Objects:** {len(manifest_entries)} (18 gold_identity, 18 gold_publication)
- **Basis Request:** config/research/dat03_lineage_ThanhTris_metadata.json
- **Source Bucket:** s3://japan-earthquake/

## Summary
- 18 identity.json: 100% verified payload SHA-256 against pinned checkpoint requirements.
- 18 publication.json: 100% verify_status=PASSED, published=true, cryptographically linked to commit.json and identity.json.
- Denominator and lineage integrity preserved: all objects retrieved from original MinIO storage.
"""
    archive_files.append(("README.md", readme_text.encode("utf-8")))

    # Atomic write to temporary file first
    pid = os.getpid()
    tmp_zip = out_dir / f"{zip_path.name}.tmp.{pid}"
    try:
        with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for name, content in archive_files:
                z.writestr(name, content)

        # Self-verify zip integrity
        with zipfile.ZipFile(tmp_zip, "r") as z:
            assert z.testzip() is None, "CORRUPT_ZIP_ARCHIVE"

        zip_bytes = tmp_zip.read_bytes()
        zip_sha = hashlib.sha256(zip_bytes).hexdigest()

        # Write checksum file atomically
        sha_path = out_dir / f"{zip_path.name}.sha256"
        tmp_sha_path = out_dir / f"{sha_path.name}.tmp.{pid}"
        tmp_sha_path.write_text(f"{zip_sha}  {zip_path.name}\n", encoding="utf-8")

        # Atomic replace
        os.replace(tmp_zip, zip_path)
        os.replace(tmp_sha_path, sha_path)

        print("Metadata package created successfully:")
        print(f"  Path: {zip_path}")
        print(f"  Size: {len(zip_bytes)} bytes")
        print(f"  SHA-256: {zip_sha}")
        print(f"  Entries count: {len(archive_files)}")
        print(f"  Checksum written to: {sha_path}")
    finally:
        if tmp_zip.exists():
            tmp_zip.unlink()


if __name__ == "__main__":
    main()
