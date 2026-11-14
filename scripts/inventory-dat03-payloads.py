#!/usr/bin/env python3
"""Bounded offline byte inventory, not Parquet ETL or portable Gold acceptance."""
import argparse
from collections import Counter
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import zipfile

PREFIX = "s3://japan-earthquake/"
SHA = re.compile(r"[0-9a-f]{64}\Z")
CHUNK = 1024 * 1024
MAX_ARCHIVES = 8
MAX_ARCHIVE_BYTES = 1024**3
MAX_ENTRIES = 20000
MAX_MEMBER_BYTES = 256 * 1024**2
MAX_DECODED_BYTES = 4 * 1024**3


def require(ok, code):
    if not ok:
        raise ValueError(code)


def safe_path(value):
    require(isinstance(value, str) and value and not value.startswith("/")
            and not any(c in value for c in ("\\", ":", "\0"))
            and all(p not in ("", ".", "..") for p in value.split("/")), "UNSAFE_PATH")
    return value


def no_symlinks(path):
    # Check original components, before resolve() can erase the link.
    path = Path(os.path.abspath(path))
    require(not any(p.is_symlink() for p in (path, *path.parents)), "SYMLINK_PATH")
    return path


def pairs(items):
    result = {}
    for k, v in items:
        require(k not in result, "DUPLICATE_JSON_KEY")
        result[k] = v
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False, "NONFINITE_JSON"))


def stream_sha(stream, limit):
    h, count = hashlib.sha256(), 0
    while True:
        chunk = stream.read(CHUNK)
        if not chunk:
            return h.hexdigest(), count
        count += len(chunk)
        require(count <= limit, "STREAM_BYTE_LIMIT")
        h.update(chunk)


def load_request(path, expected_sha):
    require(isinstance(expected_sha, str) and SHA.fullmatch(expected_sha), "INVALID_REQUEST_SHA")
    path = no_symlinks(path)
    require(path.is_file() and path.stat().st_size <= 1024**2, "REQUEST_SIZE_LIMIT")
    data = path.read_bytes()
    require(hashlib.sha256(data).hexdigest() == expected_sha, "REQUEST_PIN_MISMATCH")
    req = decode(data)
    require(isinstance(req, dict) and isinstance(req.get("objects"), list)
            and 0 < len(req["objects"]) <= 10000, "INVALID_REQUEST")
    objects, by_key, content_sizes = [], {}, {}
    for item in req["objects"]:
        require(isinstance(item, dict), "INVALID_OBJECT")
        uri, sha, size = item.get("uri"), item.get("sha256"), item.get("byte_size")
        require(isinstance(uri, str) and uri.startswith(PREFIX), "FOREIGN_URI")
        key = safe_path(uri[len(PREFIX):])
        require(key.startswith("silver/bundles/") and key.endswith(".parquet")
                and item.get("kind") == "silver_payload", "UNSUPPORTED_PAYLOAD")
        require(isinstance(sha, str) and SHA.fullmatch(sha), "INVALID_OBJECT_SHA")
        require(type(size) is int and 0 <= size <= MAX_MEMBER_BYTES, "INVALID_OBJECT_SIZE")
        require(key not in by_key, "DUPLICATE_OBJECT_URI")
        require(sha not in content_sizes or content_sizes[sha] == size, "CONFLICTING_CONTENT_SIZE")
        content_sizes[sha] = size
        row = {"uri": uri, "sha256": sha, "kind": "silver_payload", "byte_size": size}
        by_key[key] = row
        objects.append(row)
    require(req.get("object_count", len(objects)) == len(objects), "REQUEST_COUNT_MISMATCH")
    return req, objects, by_key


def inventory(request_path, request_sha, archives):
    """archives is [(local_path, independently expected ZIP SHA), ...].

    Hash compressed files and candidate member streams without extraction. A
    content match at another path is reusable bytes, NOT original S3 existence
    or permission to replace the original key. No footer/schema/row validation.
    """
    req, objects, by_key = load_request(request_path, request_sha)
    require(0 < len(archives) <= MAX_ARCHIVES, "ARCHIVE_COUNT_LIMIT")
    wanted = {(o["sha256"], o["byte_size"]) for o in objects}
    sizes = {o["byte_size"] for o in objects}
    available, summaries, seen_archives = {}, [], set()
    decoded_total = 0
    # Keep the same opened descriptors for ZIP hash and member scan.
    with ExitStack() as stack:
        for path, pin in archives:
            require(isinstance(pin, str) and SHA.fullmatch(pin), "INVALID_ARCHIVE_SHA")
            require(pin not in seen_archives, "DUPLICATE_ARCHIVE")
            seen_archives.add(pin)
            path = no_symlinks(path)
            require(path.is_file() and path.stat().st_size <= MAX_ARCHIVE_BYTES, "ARCHIVE_SIZE_LIMIT")
            raw = stack.enter_context(path.open("rb"))
            before = os.fstat(raw.fileno())
            actual_sha, compressed_bytes = stream_sha(raw, MAX_ARCHIVE_BYTES)
            require(actual_sha == pin, "ARCHIVE_PIN_MISMATCH")
            raw.seek(0)
            zf = stack.enter_context(zipfile.ZipFile(raw))
            infos = zf.infolist()
            require(len(infos) <= MAX_ENTRIES, "ENTRY_COUNT_LIMIT")
            seen, candidates, parquet_count = set(), [], 0
            for info in infos:
                name = safe_path(info.filename[:-1] if info.is_dir() else info.filename)
                require(name not in seen, "DUPLICATE_ZIP_MEMBER")
                seen.add(name)
                mode = info.external_attr >> 16
                require(not stat.S_ISLNK(mode) and not (info.flag_bits & 1), "UNSAFE_ZIP_MEMBER")
                require(stat.S_IFMT(mode) in (0, stat.S_IFREG, stat.S_IFDIR), "SPECIAL_ZIP_MEMBER")
                require(info.file_size <= MAX_MEMBER_BYTES, "MEMBER_SIZE_LIMIT")
                decoded_total += info.file_size
                require(decoded_total <= MAX_DECODED_BYTES, "DECODED_BYTE_LIMIT")
                if info.is_dir():
                    continue
                physical_key = name[len("metadata/"):] if name.startswith("metadata/") else name
                if physical_key in by_key:
                    require(info.file_size == by_key[physical_key]["byte_size"], "EXACT_PATH_SIZE_MISMATCH")
                if name.endswith(".parquet"):
                    parquet_count += 1
                    if info.file_size in sizes or physical_key in by_key:
                        candidates.append((info, name, physical_key))
            hashed_bytes = 0
            for info, name, physical_key in candidates:
                with zf.open(info) as stream:
                    member_sha, count = stream_sha(stream, min(info.file_size, MAX_MEMBER_BYTES))
                require(count == info.file_size, "MEMBER_LENGTH_MISMATCH")
                hashed_bytes += count
                if physical_key in by_key:
                    require(member_sha == by_key[physical_key]["sha256"], "EXACT_PATH_SHA_MISMATCH")
                if (member_sha, count) in wanted:
                    available.setdefault((member_sha, count), []).append(
                        {"archive_sha256": pin, "member": name, "byte_size": count})
            after = os.fstat(raw.fileno())
            require((before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    == (after.st_size, after.st_mtime_ns, after.st_ctime_ns), "ARCHIVE_CHANGED_DURING_SCAN")
            summaries.append({"sha256": pin, "compressed_bytes": compressed_bytes,
                              "entries": len(infos), "parquet_members": parquet_count,
                              "candidate_members_hashed": len(candidates), "candidate_bytes_hashed": hashed_bytes})
    result = []
    for obj in sorted(objects, key=lambda o: o["uri"]):
        sources = available.get((obj["sha256"], obj["byte_size"]), [])
        result.append({**obj, "status": "BYTE_IDENTICAL_AVAILABLE" if sources
                       else "NOT_BYTE_IDENTICAL_IN_SCANNED_ARCHIVES", "sources": sources})
    missing = [o for o in result if not o["sources"]]
    missing_contents = {(o["sha256"], o["byte_size"]) for o in missing}
    return {"version": "dat03-payload-byte-inventory-v1", "owner": req.get("owner"),
            "request_sha256": request_sha, "archives": summaries,
            "expected_references": len(result), "expected_reference_bytes": sum(o["byte_size"] for o in result),
            "expected_unique_contents": len(wanted), "matched_references": len(result) - len(missing),
            "missing_references": len(missing), "missing_reference_bytes": sum(o["byte_size"] for o in missing),
            "missing_unique_contents": len(missing_contents),
            "missing_unique_bytes": sum(size for _, size in missing_contents),
            "counts": dict(Counter(o["status"] for o in result)), "objects": result,
            "delta_objects": [{k: o[k] for k in ("uri", "sha256", "kind", "byte_size")} for o in missing],
            "payload_bytes_available": not missing, "s3_keys_verified": False,
            "parquet_semantics_verified": False, "full_transitive_closure_verified": False,
            "portable_import_ready": False, "official_export_ready": False,
            "note": "Missing means no byte-identical member in the scanned archives, not data lost. "
                    "Repartitioned/equivalent rows do not replace original pinned bytes. "
                    "Sources map reusable content only; no extraction, S3 write, ETL or Gold acceptance."}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--request", type=Path, required=True)
    p.add_argument("--request-sha256", required=True)
    p.add_argument("--archive", type=Path, action="append", required=True)
    p.add_argument("--archive-sha256", action="append", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    require(len(args.archive) == len(args.archive_sha256), "ARCHIVE_PIN_COUNT_MISMATCH")
    output = no_symlinks(args.output)
    require(not output.exists(), "OUTPUT_EXISTS")
    result = inventory(args.request, args.request_sha256, list(zip(args.archive, args.archive_sha256)))
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps({k: result[k] for k in ("expected_references", "matched_references",
                                            "missing_references", "missing_unique_bytes",
                                            "portable_import_ready", "official_export_ready")}))
    return 0 if result["payload_bytes_available"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
