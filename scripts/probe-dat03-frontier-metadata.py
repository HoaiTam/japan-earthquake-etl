#!/usr/bin/env python3
"""Probe the 18 identity.json and 18 publication.json objects from MinIO or an archive for Trí."""
import argparse
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
DEFAULT_REQ = ROOT / "config/research/dat03_lineage_ThanhTris_metadata.json"
DEFAULT_OUT = ROOT / "staging/dat03_lineage_probe_frontier.json"


def parse_args():
    parser = argparse.ArgumentParser(description="Probe metadata objects from MinIO or archive")
    parser.add_argument("--request", type=str, default=str(DEFAULT_REQ), help="Path to request JSON")
    parser.add_argument("--archive", type=str, default=None, help="Optional path to ZIP archive to read offline")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUT), help="Path to output probe JSON")
    return parser.parse_args()


def get_reader(args, bucket):
    if args.archive:
        archive_path = Path(args.archive)
        if not archive_path.exists():
            raise FileNotFoundError(f"Archive not found: {archive_path}")
        zf = zipfile.ZipFile(archive_path, "r")

        def read_bytes(rel_key):
            # Try directly, or with metadata/ prefix
            if rel_key in zf.namelist():
                return zf.read(rel_key)
            alt_key = f"metadata/{rel_key}"
            if alt_key in zf.namelist():
                return zf.read(alt_key)
            raise KeyError(f"Key {rel_key} not found in archive {archive_path}")

        return read_bytes, zf
    else:
        if boto3 is None:
            raise RuntimeError("boto3 is required when --archive is not provided")
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY"),
            region_name=os.environ.get("S3_REGION", "us-east-1"),
        )

        def read_bytes(rel_key):
            resp = s3.get_object(Bucket=bucket, Key=rel_key)
            return resp["Body"].read()

        return read_bytes, None


def main():
    args = parse_args()
    req_path = Path(args.request)
    with open(req_path, "r", encoding="utf-8") as f:
        req = json.load(f)

    bucket = os.environ.get("DATA_BUCKET", "japan-earthquake")
    read_fn, zf_ctx = get_reader(args, bucket)

    print(f"Loaded {len(req['objects'])} identity targets and {len(req['unpinned_publication_uris'])} publication targets.")

    identity_results = []
    for item in req["objects"]:
        uri = item["uri"]
        expected_sha = item["sha256"]
        rel_key = uri.replace(f"s3://{bucket}/", "")
        try:
            data = read_fn(rel_key)
            file_sha = hashlib.sha256(data).hexdigest()
            parsed = json.loads(data.decode("utf-8"))
            internal_sha = parsed.get("identity_sha256")

            # Recompute payload SHA following GoldIcebergWriter serialization contract
            needle = b',"identity_sha256":"' + (internal_sha or "").encode("ascii") + b'"}'
            valid_serialization = data.endswith(needle)
            recomputed_payload_sha = None
            payload_matches_expected = False
            payload_matches_internal = False

            if valid_serialization and internal_sha:
                payload_bytes = data[:-len(needle)] + b'}'
                recomputed_payload_sha = hashlib.sha256(payload_bytes).hexdigest()
                payload_matches_internal = (recomputed_payload_sha == internal_sha)
                payload_matches_expected = (recomputed_payload_sha == expected_sha)

            identity_results.append({
                "uri": uri,
                "key": rel_key,
                "byte_size": len(data),
                "file_sha256": file_sha,
                "internal_identity_sha256": internal_sha,
                "payload_sha256": recomputed_payload_sha,
                "expected_sha256": expected_sha,
                "valid_serialization": valid_serialization,
                "internal_matches_expected": (internal_sha == expected_sha),
                "payload_matches_internal": payload_matches_internal,
                "payload_matches_expected": payload_matches_expected,
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
            data = read_fn(rel_key)
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
                "snapshot_bundle": parsed.get("snapshot_bundle"),
                "checks": parsed.get("checks"),
            })
        except Exception as e:
            pub_results.append({
                "uri": uri,
                "key": rel_key,
                "error": str(e),
            })

    if zf_ctx:
        zf_ctx.close()

    summary = {
        "identities_found": sum("byte_size" in r for r in identity_results),
        "identities_internal_match": sum(r.get("internal_matches_expected", False) for r in identity_results),
        "identities_payload_match": sum(r.get("payload_matches_expected", False) for r in identity_results),
        "identities_file_match": sum(r.get("file_matches_expected", False) for r in identity_results),
        "publications_found": sum("byte_size" in r for r in pub_results),
        "publications_verified_passed": sum(r.get("verify_status") == "PASSED" for r in pub_results),
        "publications_published_true": sum(r.get("published") is True for r in pub_results),
    }

    print("PROBE SUMMARY:", json.dumps(summary, indent=2))

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({"summary": summary, "identities": identity_results, "publications": pub_results}, f, indent=2)
    print(f"Results saved to {output_path}")


if __name__ == "__main__":
    main()
