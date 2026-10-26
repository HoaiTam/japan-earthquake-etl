#!/usr/bin/env python3
"""Offline byte/ZIP verification of the pinned JMA source handoff, not ETL or coverage acceptance."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import stat
import zipfile

YEARS = tuple(range(2000, 2025))
SUPPORT = {"README.md", "verify_sources.py", "checksums.sha256"}
MAX_MANIFEST = 128 * 1024
MAX_ARCHIVE = 32 * 1024**2
MAX_MEMBER = 128 * 1024**2


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "SOURCE_DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    def bad(_):
        raise ValueError("SOURCE_NONFINITE_JSON")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad)


def verify(root, expected_manifest_sha256):
    root = Path(root).absolute()
    require(not any(p.is_symlink() for p in (root, *root.parents)), "SOURCE_SYMLINK_REJECTED")
    require(root.is_dir() and root == root.resolve(), "SOURCE_DIRECTORY_REQUIRED")
    require(re.fullmatch(r"[a-f0-9]{64}", expected_manifest_sha256 or ""), "SOURCE_EXTERNAL_PIN_REQUIRED")
    metadata = root / "manifest.json"
    require(metadata.is_file() and not metadata.is_symlink()
            and 0 < metadata.stat().st_size <= MAX_MANIFEST, "SOURCE_MANIFEST_REQUIRED")
    require(sha(metadata) == expected_manifest_sha256, "SOURCE_MANIFEST_PIN_MISMATCH")
    manifest = strict_json(metadata.read_bytes())
    require(manifest.get("version") == "dat03-jma-source-handoff-v1"
            and manifest.get("artifact_kind") == "PINNED_JMA_SOURCE_ARCHIVES"
            and manifest.get("source_system") == "JMA_BULLETIN"
            and manifest.get("research_ready") is False
            and manifest.get("gold_published") is False
            and manifest.get("bronze_ready") is False
            and manifest.get("coverage_verified") is False, "SOURCE_ONLY_REQUIRED")
    archives = manifest.get("archives")
    require(isinstance(archives, list) and len(archives) == 25
            and all(type(a.get("year")) is int for a in archives)
            and [a["year"] for a in archives] == list(YEARS), "SOURCE_EXACT_YEARS_REQUIRED")
    expected_archives = {f"sources/h{year}/h{year}.zip" for year in YEARS}
    files = manifest.get("files")
    require(isinstance(files, dict) and set(files) == SUPPORT | expected_archives,
            "SOURCE_EXACT_FILE_SET_REQUIRED")
    expected_files = set(files) | {"manifest.json"}
    expected_dirs = {str(parent) for name in expected_files for parent in Path(name).parents
                     if str(parent) != "."}
    found = set()
    for path in root.rglob("*"):
        require(not path.is_symlink(), "SOURCE_SYMLINK_REJECTED")
        name = path.relative_to(root).as_posix()
        if path.is_dir():
            require(name in expected_dirs, "SOURCE_EXTRA_DIRECTORY")
        else:
            require(path.is_file() and path.stat().st_nlink == 1, "SOURCE_UNSAFE_FILE")
            found.add(name)
    require(found == expected_files, "SOURCE_EXACT_FILE_SET_REQUIRED")
    total = 0
    for name, entry in files.items():
        path = root / name
        require(type(entry.get("bytes")) is int and 0 < entry["bytes"] <= MAX_ARCHIVE
                and re.fullmatch(r"[a-f0-9]{64}", entry.get("sha256", ""))
                and path.stat().st_size == entry["bytes"] and sha(path) == entry["sha256"],
                "SOURCE_FILE_CHANGED:" + name)
    checks = "".join(f"{files[name]['sha256']}  {name}\n" for name in sorted(files)
                     if name != "checksums.sha256")
    require((root / "checksums.sha256").read_text() == checks, "SOURCE_CHECKSUM_LIST_CHANGED")
    for item in archives:
        year = item["year"]
        name = f"sources/h{year}/h{year}.zip"
        require(item.get("path") == name and item.get("member") == f"h{year}"
                and item.get("role") == ("BOUNDARY_SOURCE_ONLY" if year == 2024 else "RESEARCH_SOURCE")
                and item.get("sha256") == files[name]["sha256"]
                and item.get("bytes") == files[name]["bytes"]
                and type(item.get("member_bytes")) is int and 0 < item["member_bytes"] <= MAX_MEMBER,
                "SOURCE_ARCHIVE_METADATA_CHANGED")
        with zipfile.ZipFile(root / name) as archive:
            entries = archive.infolist()
            require(len(entries) == 1, "SOURCE_ZIP_EXACT_MEMBER_REQUIRED")
            member = entries[0]
            require(member.filename == item["member"] and not member.is_dir()
                    and not member.flag_bits & 1 and not stat.S_ISLNK(member.external_attr >> 16)
                    and member.file_size == item["member_bytes"], "SOURCE_ZIP_LAYOUT_CHANGED")
            read = 0
            with archive.open(member) as stream:
                while block := stream.read(1024 * 1024):
                    read += len(block)
                    require(read <= item["member_bytes"], "SOURCE_MEMBER_SIZE_CHANGED")
            require(read == item["member_bytes"], "SOURCE_MEMBER_SIZE_CHANGED")
        total += item["bytes"]
    require(total == manifest.get("compressed_archive_bytes"), "SOURCE_TOTAL_CHANGED")
    return {"status": "PINNED_JMA_SOURCES_VERIFIED", "manifest_sha256": expected_manifest_sha256,
            "archives": 25, "compressed_archive_bytes": total, "years": [2000, 2024],
            "coverage_verified": False, "bronze_ready": False, "gold_published": False,
            "research_ready": False, "scope": "SOURCE_BYTES_AND_ZIP_ONLY_NOT_ETL_OR_RESEARCH"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.root, args.manifest_sha256), indent=2, sort_keys=True))
