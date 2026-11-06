#!/usr/bin/env python3
"""Extract first-hop provenance objects from MinIO for rosy179."""

import json
import os
from pathlib import Path
import zipfile
import hashlib
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "pipeline-access-key"),
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "pipeline-secret-key"),
    region_name=os.environ.get("S3_REGION", "us-east-1"),
)

# Load expected 83 URIs
with open("/tmp/rosy179_expected_83.json", "r", encoding="utf-8") as f:
    expected_uris = json.load(f)

print(f"Total objects to extract: {len(expected_uris)}")
assert len(expected_uris) == 83, f"Expected 83 objects, got {len(expected_uris)}"

out_dir = Path("/tmp/rosy179-provenance")
out_dir.mkdir(parents=True, exist_ok=True)

manifest_items = []
verified = 0

for uri, exp_sha in sorted(expected_uris.items()):
    assert uri.startswith("s3://japan-earthquake/"), f"Unexpected URI: {uri}"
    key = uri[len("s3://japan-earthquake/"):]
    obj = s3.get_object(Bucket="japan-earthquake", Key=key)
    body = obj["Body"].read()
    actual_sha = hashlib.sha256(body).hexdigest()
    if actual_sha != exp_sha:
        raise ValueError(f"SHA mismatch for {uri}: expected {exp_sha}, got {actual_sha}")

    # Save file mirroring S3 path structure
    target_p = out_dir / key
    target_p.parent.mkdir(parents=True, exist_ok=True)
    with open(target_p, "wb") as f:
        f.write(body)

    category = "bronze_manifest" if key.startswith("bronze/") else ("silver_bundle_manifest" if "bundles/" in key else "registry_json")
    manifest_items.append({
        "category": category,
        "s3_uri": uri,
        "archive_path": key,
        "bytes": len(body),
        "sha256": actual_sha,
    })
    verified += 1

print(f"Verified and saved {verified}/83 objects!")

# Create packaged ZIP
zip_out = Path("/tmp/rosy179-dat03-provenance-metadata.zip")
manifest_json = {
    "version": "dat03-provenance-package-v1",
    "owner": "rosy179",
    "range": "2018-10..2023-12",
    "month_count": 63,
    "total_objects": len(manifest_items),
    "required_objects": 83,
    "counts_by_category": {
        "bronze_manifest": sum(1 for x in manifest_items if x["category"] == "bronze_manifest"),
        "silver_bundle_manifest": sum(1 for x in manifest_items if x["category"] == "silver_bundle_manifest"),
        "registry_json": sum(1 for x in manifest_items if x["category"] == "registry_json"),
    },
    "objects": manifest_items,
}

manifest_bytes = json.dumps(manifest_json, indent=2).encode("utf-8")
manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()

with zipfile.ZipFile(zip_out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    zf.writestr("manifest.json", manifest_bytes)
    for item in manifest_items:
        zf.write(out_dir / item["archive_path"], arcname=item["archive_path"])

zip_sha = hashlib.sha256(zip_out.read_bytes()).hexdigest()

# Write required-provenance.json
req_prov_file = Path("/tmp/rosy179-required-provenance.json")
with open(req_prov_file, "w", encoding="utf-8") as f:
    json.dump({
        "owner": "rosy179",
        "total_objects": 83,
        "zip_sha256": zip_sha,
        "manifest_sha256": manifest_sha,
        "counts": manifest_json["counts_by_category"],
        "checksums": {item["archive_path"]: item["sha256"] for item in manifest_items},
    }, f, indent=2)

print(f"SUCCESS! Created {zip_out}")
print(f"Zip SHA-256: {zip_sha}")
print(f"Zip size: {zip_out.stat().st_size} bytes")
print(f"Category breakdown: {manifest_json['counts_by_category']}")
