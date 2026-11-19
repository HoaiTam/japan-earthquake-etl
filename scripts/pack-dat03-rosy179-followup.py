#!/usr/bin/env python3
"""Package the pinned rosy179 DAT-03 metadata; no ETL or publication."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import importlib.util
from io import BytesIO
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from urllib.parse import urlsplit
import zipfile

try:
    import boto3
except ImportError:
    boto3 = None

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REQUEST_PATH = ROOT / "config/research/dat03_lineage_rosy179_followup_20261114.json"
DEFAULT_OUTPUT_ZIP = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-dat03-lineage-followup.zip"
DEFAULT_RAW_DIR = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-dat03-followup-raw"
DEFAULT_BUCKET = "japan-earthquake"
MAX_REQUEST = 1024**2
MAX_OBJECT = 2 * 1024**2
MAX_ARCHIVE = 8 * 1024**2
MAX_EXPANDED = 64 * 1024**2
FIXED_TIME = (2026, 11, 14, 21, 4, 0)

_spec = importlib.util.spec_from_file_location("dat03_lineage", Path(__file__).with_name("audit-dat03-lineage.py"))
lineage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(lineage)
require = lineage.require
digest = lineage.digest
RUN = r"orc07-[a-f0-9]{40}"
KEYS = {
    "gold_identity": re.compile(rf"silver/gold-commits/({RUN})/identity\.json"),
    "gold_commit": re.compile(rf"silver/gold-commits/({RUN})/commit\.json"),
    "gold_publication": re.compile(rf"silver/gold-verification/({RUN})/publication\.json"),
}


def _checked_path(path: Path) -> Path:
    # Check raw components first; resolving a symlink before checking hides it.
    path = Path(path).absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)), "SYMLINK_REJECTED")
    return path.resolve()


def _read_file(path: Path, limit: int) -> bytes:
    path = _checked_path(path)
    if not path.is_file():
        raise FileNotFoundError(f"REGULAR_FILE_REQUIRED: {path.name}")
    require(path.stat().st_size <= limit, "FILE_SIZE_LIMIT")
    with path.open("rb") as stream:
        data = stream.read(limit + 1)
    require(len(data) <= limit, "FILE_SIZE_LIMIT")
    return data


def _json_object(data: bytes) -> dict:
    value = lineage.decode(data.decode("utf-8"))
    require(isinstance(value, dict), "JSON_OBJECT_REQUIRED")
    return value


def _is_sha(value) -> bool:
    return isinstance(value, str) and bool(lineage.SHA.fullmatch(value))


def _key_from_uri(uri: str, bucket: str, kind: str) -> tuple[str, str]:
    prefix = f"s3://{bucket}/"
    require(isinstance(uri, str) and uri.startswith(prefix), "FOREIGN_OR_INVALID_URI")
    key = uri[len(prefix):]
    match = KEYS[kind].fullmatch(key)
    require(match is not None, "UNSAFE_OR_UNEXPECTED_OBJECT_PATH")
    return key, match.group(1)


def _expected_objects(request_file: Path, bucket: str) -> dict:
    require(bool(re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket)), "INVALID_BUCKET")
    request = _json_object(_read_file(request_file, MAX_REQUEST))
    require(request.get("version") == "dat03-followup-original-metadata-request-v1", "INVALID_REQUEST_VERSION")
    require(request.get("owner") == "rosy179", "INVALID_REQUEST_OWNER")
    pins, publications = request.get("objects"), request.get("unpinned_publication_uris")
    require(isinstance(pins, list) and type(request.get("object_count")) is int
            and len(pins) == request["object_count"] == 24, "INVALID_PINNED_COUNT")
    require(isinstance(publications, list) and len(publications) == 23, "INVALID_PUBLICATION_COUNT")
    expected, identities, commits = {}, {}, {}
    for item in pins:
        require(isinstance(item, dict) and item.get("kind") in ("gold_identity", "gold_commit")
                and _is_sha(item.get("sha256")), "INVALID_PINNED_OBJECT")
        kind, uri, sha = item["kind"], item.get("uri"), item["sha256"]
        key, run = _key_from_uri(uri, bucket, kind)
        require(key not in expected, "DUPLICATE_REQUEST_OBJECT")
        expected[key] = {"uri": uri, "kind": kind, "expected_sha256": sha}
        (identities if kind == "gold_identity" else commits)[run] = sha
    head = request.get("head_run_id")
    require(isinstance(head, str) and len(identities) == 23 and set(commits) == {head}
            and head in identities, "INVALID_REQUEST_HEAD_OR_PINS")
    for uri in publications:
        key, run = _key_from_uri(uri, bucket, "gold_publication")
        require(key not in expected, "DUPLICATE_REQUEST_OBJECT")
        require(run in identities, "PUBLICATION_IDENTITY_NOT_REQUESTED")
        expected[key] = {"uri": uri, "kind": "gold_publication", "run_id": run,
                         "identity_sha256": identities[run], "commit_sha256": commits.get(run)}
    return expected


def get_s3_client(endpoint_url: str | None = None):
    minio_access, minio_secret = os.getenv("MINIO_ACCESS_KEY"), os.getenv("MINIO_SECRET_KEY")
    aws_access, aws_secret = os.getenv("AWS_ACCESS_KEY_ID"), os.getenv("AWS_SECRET_ACCESS_KEY")
    token = None
    if minio_access is not None or minio_secret is not None:
        access, secret = minio_access, minio_secret
    else:
        access, secret = aws_access, aws_secret
        token = os.getenv("AWS_SESSION_TOKEN")
    if not access or not access.strip() or not secret or not secret.strip():
        raise RuntimeError("MISSING_OR_INCOMPLETE_S3_CREDENTIALS: configure one complete MinIO or AWS pair")
    if boto3 is None:
        raise RuntimeError("boto3 is required for explicit S3 mode")
    endpoint = endpoint_url or os.getenv("MINIO_ENDPOINT") or os.getenv("MINIO_ENDPOINT_URL") or "http://localhost:9000"
    parsed = urlsplit(endpoint)
    require(parsed.scheme in {"http", "https"} and parsed.hostname and parsed.username is None and parsed.password is None
            and not parsed.query and not parsed.fragment, "INVALID_S3_ENDPOINT")
    kwargs = {"endpoint_url": endpoint, "aws_access_key_id": access, "aws_secret_access_key": secret,
              "region_name": os.getenv("S3_REGION") or "us-east-1"}
    if token:
        kwargs["aws_session_token"] = token
    return boto3.client("s3", **kwargs)


def fetch_bytes(key: str, bucket: str = DEFAULT_BUCKET, source_dir: Path | None = None, s3_client=None) -> bytes:
    require(isinstance(key, str) and any(pattern.fullmatch(key) for pattern in KEYS.values()), "UNSAFE_OBJECT_PATH")
    if source_dir is not None:
        root = _checked_path(source_dir)
        if not root.is_dir():
            raise FileNotFoundError("SOURCE_DIR_NOT_FOUND")
        path = _checked_path(root / key)
        require(path.is_relative_to(root), "PATH_ESCAPES_SOURCE_ROOT")
        # A missing local file must never fall back to a network read.
        return _read_file(path, MAX_OBJECT)
    if s3_client is None:
        raise RuntimeError("EXPLICIT_S3_CLIENT_REQUIRED")
    response = s3_client.get_object(Bucket=bucket, Key=key)
    stream = response["Body"]
    try:
        length = response.get("ContentLength")
        require(type(length) is int and 0 <= length <= MAX_OBJECT, "S3_OBJECT_SIZE_LIMIT")
        data = stream.read(length + 1)
        require(len(data) == length, "S3_OBJECT_LENGTH_MISMATCH")
        return data
    finally:
        stream.close()


def _entry(key: str, content: bytes, pin: dict) -> dict:
    sha = digest(content)
    entry = {"uri": pin["uri"], "relative_path": key, "kind": pin["kind"], "size": len(content), "sha256": sha}
    if pin["kind"] == "gold_identity":
        payload_sha, _ = lineage.verify_gold_identity(content, pin["expected_sha256"])
        entry.update(expected_sha256=pin["expected_sha256"], identity_payload_sha256=payload_sha,
                     internal_identity_sha256=payload_sha, status="MATCHED")
    elif pin["kind"] == "gold_commit":
        require(sha == pin["expected_sha256"], "COMMIT_SHA_MISMATCH")
        entry.update(expected_sha256=pin["expected_sha256"], status="MATCHED")
    else:
        pub = _json_object(content)
        require(pub.get("verify_status") == "PASSED" and pub.get("published") is True, "UNPUBLISHED_PUBLICATION")
        require(pub.get("gold_run_id") == pin["run_id"] and pub.get("identity_sha256") == pin["identity_sha256"],
                "PUBLICATION_LINEAGE_MISMATCH")
        require(_is_sha(pub.get("bundle_sha256")) and
                (pin["commit_sha256"] is None or pub["bundle_sha256"] == pin["commit_sha256"]),
                "PUBLICATION_COMMIT_MISMATCH")
        entry.update({field: pub.get(field) for field in ("publication_id", "verify_status", "published", "event_count",
                                                         "verified_at_utc", "identity_sha256", "bundle_sha256")})
        entry["status"] = "FETCHED_ORIGINAL"
    return entry


def _legacy_entry(actual: dict) -> dict:
    # The handed-off November ZIP used a minimal inventory. Validate exactly
    # that shape too, after rechecking all payloads and trusted pins above.
    return {
        **{field: actual[field] for field in ("uri", "relative_path", "kind", "size", "sha256")},
        "expected_sha256": actual.get("expected_sha256"),
    }


def _verify_zip(zp: Path, expected: dict) -> dict:
    raw = _read_file(zp, MAX_ARCHIVE)
    # Inspect and hash the same archive bytes, rather than opening the path again.
    with zipfile.ZipFile(BytesIO(raw)) as zf:
        infos, names, expanded = zf.infolist(), set(), 0
        require(len(infos) == len(expected) + 1, "ZIP_OBJECT_SET_MISMATCH")
        for info in infos:
            name = info.filename
            require(info.orig_filename == name and name not in names, "DUPLICATE_OR_ALIASED_ZIP_MEMBER")
            require(name == "manifest.json" or any(pattern.fullmatch(name) for pattern in KEYS.values()),
                    "UNSAFE_ZIP_MEMBER_PATH")
            require(not info.is_dir() and stat.S_IFMT(info.external_attr >> 16) in (0, stat.S_IFREG)
                    and not info.flag_bits & 1 and info.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED),
                    "UNSAFE_ZIP_MEMBER_TYPE")
            expanded += info.file_size
            require(info.file_size <= MAX_OBJECT and expanded <= MAX_EXPANDED, "ZIP_EXPANDED_SIZE_LIMIT")
            names.add(name)
        require(names == {"manifest.json", *expected}, "ZIP_OBJECT_SET_MISMATCH")

        # read(info) checks CRC; never decompress until every central entry is checked.
        manifest = _json_object(zf.read("manifest.json"))
        entries = manifest.get("objects")
        require(manifest.get("owner") == "rosy179" and manifest.get("total_objects") == len(expected)
                and isinstance(entries, list) and len(entries) == len(expected), "MANIFEST_INVENTORY_INVALID")
        require(type(manifest.get("total_bytes")) is int and
                manifest["total_bytes"] == expanded - zf.getinfo("manifest.json").file_size, "MANIFEST_TOTAL_BYTES_MISMATCH")
        seen, schemas = set(), set()
        for item in entries:
            require(isinstance(item, dict), "MANIFEST_ENTRY_INVALID")
            key = item.get("relative_path")
            require(isinstance(key, str) and key in expected and key not in seen, "MANIFEST_OBJECT_SET_MISMATCH")
            data = zf.read(key)
            actual = _entry(key, data, expected[key])
            encoded = json.dumps(item, sort_keys=True)
            if encoded == json.dumps(actual, sort_keys=True):
                schemas.add("FULL")
            elif encoded == json.dumps(_legacy_entry(actual), sort_keys=True):
                schemas.add("LEGACY_MINIMAL")
            else:
                raise ValueError("MANIFEST_OBJECT_MISMATCH")
            seen.add(key)
        require(seen == set(expected), "MANIFEST_OBJECT_SET_MISMATCH")
        require(len(schemas) == 1, "MANIFEST_SCHEMA_MIXED")

    counts = Counter(pin["kind"] for pin in expected.values())
    return {"status": "VERIFIED", "archive": str(zp), "bytes": len(raw), "sha256": digest(raw),
            "pinned_matched": counts["gold_identity"] + counts["gold_commit"],
            "pubs_matched": counts["gold_publication"], "total_entries": len(infos),
            "manifest_schema": schemas.pop(),
            "publication_verification": "PACKAGE_SELF_CHECK_ONLY", "publication_authenticity_verified": False}


def verify_followup_archive(zip_path: Path = DEFAULT_OUTPUT_ZIP, request_file: Path = DEFAULT_REQUEST_PATH,
                            bucket: str = DEFAULT_BUCKET) -> dict:
    return _verify_zip(_checked_path(zip_path), _expected_objects(request_file, bucket))


def _publish_pair(staged_zip: Path, staged_sha: Path, out: Path, sidecar: Path, existing: bool) -> None:
    if existing:
        previous = staged_zip.with_name("previous.zip")
        os.link(out, previous)
        os.replace(staged_zip, out)
        try:
            os.replace(staged_sha, sidecar)
        except OSError:
            os.replace(previous, out)
            raise
    else:
        # link is atomic and never clobbers an existing file.
        os.link(staged_zip, out)
        try:
            os.link(staged_sha, sidecar)
        except OSError:
            if out.is_file() and not out.is_symlink() and out.samefile(staged_zip):
                out.unlink()
            raise


def _checksum_line(sha: str, name: str) -> bytes:
    return f"{sha}  {name}\n".encode("utf-8")


def pack_followup_archive(request_file: Path = DEFAULT_REQUEST_PATH, output_zip: Path = DEFAULT_OUTPUT_ZIP,
                          source_dir: Path | None = DEFAULT_RAW_DIR, endpoint_url: str | None = None,
                          bucket: str = DEFAULT_BUCKET, overwrite: bool = False) -> dict:
    expected = _expected_objects(request_file, bucket)
    out = _checked_path(output_zip)
    require(bool(re.fullmatch(r"[A-Za-z0-9_.-]+\.zip", out.name)), "INVALID_OUTPUT_NAME")
    sidecar = _checked_path(out.with_name(out.name + ".sha256"))
    out.parent.mkdir(parents=True, exist_ok=True)
    lock = out.with_name(out.name + ".lock")
    with lock.open("x", encoding="utf-8"):
        pass
    try:
        existing = out.exists()
        if existing != sidecar.exists():
            raise FileExistsError("OUTPUT_PAIR_INCOMPLETE")
        old_sha = None
        if existing:
            old_sha = digest(_read_file(out, MAX_ARCHIVE))
            require(_read_file(sidecar, 4096).replace(b"\r\n", b"\n") == _checksum_line(old_sha, out.name),
                    "OUTPUT_CHECKSUM_MISMATCH")

        s3 = get_s3_client(endpoint_url) if source_dir is None else None
        manifest_entries, total = [], 0
        with tempfile.TemporaryDirectory(prefix=".dat03-followup-", dir=out.parent) as tmp:
            staged_zip = Path(tmp) / out.name
            staged_sha = Path(tmp) / sidecar.name
            with zipfile.ZipFile(staged_zip, "x", compression=zipfile.ZIP_DEFLATED) as archive:
                def add(name: str, data: bytes) -> None:
                    info = zipfile.ZipInfo(name, FIXED_TIME)
                    info.create_system = 3
                    info.compress_type = zipfile.ZIP_DEFLATED
                    info.external_attr = 0o644 << 16
                    archive.writestr(info, data)

                for key, pin in expected.items():
                    data = fetch_bytes(key, bucket, source_dir, s3)
                    total += len(data)
                    require(total <= MAX_EXPANDED, "ZIP_EXPANDED_SIZE_LIMIT")
                    manifest_entries.append(_entry(key, data, pin))
                    add(key, data)
                add("manifest.json", json.dumps({
                    "owner": "rosy179", "source": f"MinIO original objects (bucket: {bucket})",
                    "total_objects": len(expected), "total_bytes": total, "objects": manifest_entries,
                }, indent=2, ensure_ascii=False, allow_nan=False).encode("utf-8"))

            report = _verify_zip(staged_zip, expected)
            sha = report["sha256"]
            if old_sha == sha:
                status = "REUSED"
            else:
                if existing and not overwrite:
                    raise FileExistsError(f"OUTPUT_ALREADY_EXISTS: {out.name}")
                staged_sha.write_bytes(_checksum_line(sha, out.name))
                _publish_pair(staged_zip, staged_sha, out, sidecar, existing)
                status = "SUCCESS"
        return {"status": status, "archive": str(out), "bytes": report["bytes"], "sha256": sha,
                "object_count": len(expected)}
    finally:
        lock.unlink()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--pack", action="store_true", help="Pack and verify locally (default)")
    action.add_argument("--verify", action="store_true", help="Read-only archive verification")
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--source-dir", type=Path, default=DEFAULT_RAW_DIR, help="Directory of original raw objects")
    source.add_argument("--s3", action="store_true", help="Explicitly fetch requested objects from MinIO/S3")
    parser.add_argument("--output-zip", type=Path, default=DEFAULT_OUTPUT_ZIP)
    parser.add_argument("--request-file", type=Path, default=DEFAULT_REQUEST_PATH)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--endpoint-url", help="S3 endpoint used only with --s3")
    parser.add_argument("--overwrite", action="store_true", help="Opt in to replacing a valid existing ZIP/checksum pair")
    args = parser.parse_args(argv)
    if args.endpoint_url and not args.s3:
        parser.error("--endpoint-url requires --s3")
    if args.verify:
        result = verify_followup_archive(args.output_zip, args.request_file, args.bucket)
    else:
        result = pack_followup_archive(args.request_file, args.output_zip, None if args.s3 else args.source_dir,
                                       args.endpoint_url, args.bucket, overwrite=args.overwrite)
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
