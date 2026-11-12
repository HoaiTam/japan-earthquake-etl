#!/usr/bin/env python3
"""Probe the 18 identity.json and 18 publication.json objects from MinIO for Trí."""
import hashlib
import json
import os
from pathlib import Path
import sys

try:
    import boto3
except ImportError:
    print("ERROR: boto3 is required", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]
REQ_PATH = ROOT / "config/research/dat03_lineage_ThanhTris_metadata.json"

with open(REQ_PATH, "r", encoding="utf-8") as f:
    req = json.load(f)

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
    aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
    region_name=os.environ.get("S3_REGION", "us-east-1"),
)
bucket = os.environ.get("DATA_BUCKET", "japan-earthquake")

print(f"Loaded {len(req['objects'])} identity targets and {len(req['unpinned_publication_uris'])} publication targets.")

identity_results = []
for item in req["objects"]:
    uri = item["uri"]
    expected_sha = item["sha256"]
    rel_key = uri.replace(f"s3://{bucket}/", "")
    try:
        resp = s3.get_object(Bucket=bucket, Key=rel_key)
        data = resp["Body"].read()
        file_sha = hashlib.sha256(data).hexdigest()
        parsed = json.loads(data.decode("utf-8"))
        internal_sha = parsed.get("identity_sha256")
        identity_results.append({
            "uri": uri,
            "key": rel_key,
            "byte_size": len(data),
            "file_sha256": file_sha,
            "internal_identity_sha256": internal_sha,
            "expected_sha256": expected_sha,
            "internal_matches_expected": (internal_sha == expected_sha),
            "file_matches_expected": (file_sha == expected_sha),
        })
    except Exception as e:
        identity_results.append({
            "uri": uri,
            "key": rel_key,
            "error": str(e),
        })

pub_results = []
for uri in req["unpinned_publication_uris"]:
    rel_key = uri.replace(f"s3://{bucket}/", "")
    try:
        resp = s3.get_object(Bucket=bucket, Key=rel_key)
        data = resp["Body"].read()
        file_sha = hashlib.sha256(data).hexdigest()
        parsed = json.loads(data.decode("utf-8"))
        pub_results.append({
            "uri": uri,
            "key": rel_key,
            "byte_size": len(data),
            "file_sha256": file_sha,
            "publication_id": parsed.get("publication_id"),
            "verify_status": parsed.get("verify_status"),
            "published": parsed.get("published"),
            "event_count": parsed.get("event_count"),
            "verified_at_utc": parsed.get("verified_at_utc"),
            "identity_sha256": parsed.get("identity_sha256"),
            "bundle_sha256": parsed.get("bundle_sha256"),
        })
    except Exception as e:
        pub_results.append({
            "uri": uri,
            "key": rel_key,
            "error": str(e),
        })

summary = {
    "identities_found": sum("byte_size" in r for r in identity_results),
    "identities_internal_match": sum(r.get("internal_matches_expected", False) for r in identity_results),
    "identities_file_match": sum(r.get("file_matches_expected", False) for r in identity_results),
    "publications_found": sum("byte_size" in r for r in pub_results),
    "publications_verified_passed": sum(r.get("verify_status") == "PASSED" for r in pub_results),
    "publications_published_true": sum(r.get("published") is True for r in pub_results),
}

print("PROBE SUMMARY:", json.dumps(summary, indent=2))

output_path = ROOT / "staging/dat03_lineage_probe_frontier.json"
output_path.parent.mkdir(parents=True, exist_ok=True)
with open(output_path, "w", encoding="utf-8") as f:
    json.dump({"summary": summary, "identities": identity_results, "publications": pub_results}, f, indent=2)
print(f"Results saved to {output_path}")
