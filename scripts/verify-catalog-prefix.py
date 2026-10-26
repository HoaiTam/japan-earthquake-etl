#!/usr/bin/env python3
"""Verify an immutable partial processed catalog; never run ETL or grant official ML readiness."""
import argparse
from collections import Counter
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import sqlite3
import stat
import tempfile
import zipfile

VERSION = "dat03-partial-processed-catalog-v1"
KINDS = {"source_observation", "canonical_membership", "source_link"}
MAX_FILE = 32 * 1024**2
MAX_TOTAL = 1024**3


def require(ok, reason):
    if not ok:
        raise ValueError("CATALOG_KIT_" + reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    return json.loads(raw, object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False, "NONFINITE_JSON"))


def safe_name(name):
    require(isinstance(name, str) and 0 < len(name) <= 512 and "\\" not in name
            and not name.startswith("/") and str(PurePosixPath(name)) == name
            and all(p not in ("", ".", "..") for p in name.split("/")), "UNSAFE_MEMBER")
    return name


def semantic(manifest, read):
    """Read existing Spark Parquet bytes only; SQLite is a temporary verification index."""
    import pyarrow.parquet as pq
    expected_months = manifest["canonical_by_month"]
    require(isinstance(expected_months, dict) and len(expected_months) == manifest["completed_months"]
            and all(re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", m) and type(n) is int and n >= 0
                    for m, n in expected_months.items()), "MONTH_COUNTS_INVALID")
    months = Counter(); counts = Counter(); group_counts = {}; decoded = 0
    with tempfile.TemporaryDirectory(prefix="dat03-catalog-verify-") as temporary:
        with sqlite3.connect(str(Path(temporary) / "verification.sqlite")) as db:
            db.execute("PRAGMA cache_size=-32768")
            db.execute("CREATE TABLE obs (id TEXT NOT NULL)")
            db.execute("CREATE TABLE member (id TEXT NOT NULL, canonical TEXT NOT NULL)")
            for batch in manifest["batches"]:
                group = Counter(); batch_months = Counter()
                require(set(batch["canonical_by_month"]) == set(batch["affected_months"]), "BATCH_MONTHS_INVALID")
                for name in batch["files"]:
                    meta = manifest["files"][name]; kind = meta["dataset"]
                    parquet = pq.ParquetFile(io.BytesIO(read(name)))
                    require(parquet.metadata.num_rows <= 100000 and parquet.metadata.num_columns <= 256,
                            "PARQUET_BOUND")
                    require(sum(parquet.metadata.row_group(i).total_byte_size
                                for i in range(parquet.metadata.num_row_groups)) <= 128 * 1024**2,
                            "PARQUET_DECODE_BOUND")
                    group[kind] += parquet.metadata.num_rows
                    if kind == "source_observation":
                        columns = ["source_observation_id", "event_time_utc", "is_current_source_revision", "source_system"]
                    elif kind == "canonical_membership":
                        columns = ["source_observation_id", "canonical_event_id"]
                    else:
                        continue
                    for rows in parquet.iter_batches(batch_size=4096, columns=columns):
                        decoded += rows.nbytes
                        require(decoded <= 2 * 1024**3, "DECODE_TOTAL_BOUND")
                        values = rows.to_pylist()
                        if kind == "source_observation":
                            ids = []
                            for row in values:
                                event = row["event_time_utc"]
                                require(event is not None and event.tzinfo is not None
                                        and event.utcoffset().total_seconds() == 0
                                        and row["is_current_source_revision"] is True
                                        and row["source_system"] == "JMA_BULLETIN"
                                        and isinstance(row["source_observation_id"], str)
                                        and row["source_observation_id"], "OBSERVATION_INVALID")
                                month = event.strftime("%Y-%m")
                                require(month in batch["canonical_by_month"], "OUTSIDE_BATCH_MONTH")
                                batch_months[month] += 1
                                ids.append((row["source_observation_id"],))
                            db.executemany("INSERT INTO obs VALUES (?)", ids)
                        else:
                            require(all(isinstance(row[k], str) and row[k] for row in values
                                        for k in columns), "MEMBERSHIP_ID_INVALID")
                            db.executemany("INSERT INTO member VALUES (?,?)",
                                           [(row["source_observation_id"], row["canonical_event_id"]) for row in values])
                require(dict(batch_months) == batch["canonical_by_month"]
                        and group["source_observation"] == group["canonical_membership"]
                        and group["source_link"] == 0, "BATCH_RECONCILIATION_CHANGED")
                group_counts[batch["batch_start"]] = dict(group)
                counts.update(group); months.update(batch_months); db.commit()
            # Bulk-load then build exact unique indexes once. This preserves all
            # constraints while avoiding random index writes per row on slow disks.
            db.execute("CREATE UNIQUE INDEX observation_id ON obs(id)")
            db.execute("CREATE UNIQUE INDEX membership_id ON member(id)")
            db.execute("CREATE UNIQUE INDEX canonical_id ON member(canonical)")
            require(not db.execute("SELECT id FROM obs EXCEPT SELECT id FROM member LIMIT 1").fetchone()
                    and not db.execute("SELECT id FROM member EXCEPT SELECT id FROM obs LIMIT 1").fetchone(),
                    "MEMBERSHIP_COVERAGE_CHANGED")
    expected = manifest["expected_rows"]
    require(dict(months) == expected_months and counts["source_observation"] == expected
            and counts["canonical_membership"] == expected and counts["source_link"] == 0,
            "CATALOG_RECONCILIATION_CHANGED")
    return {"status": "PARTIAL_PROCESSED_CATALOG_VERIFIED", "completed_months": len(months),
            "rows": dict(counts), "canonical_by_month": dict(sorted(months.items())),
            "unique_observation_ids": expected, "unique_canonical_ids": expected,
            "membership_coverage_verified": True, "research_ready": False,
            "official_exported": False, "warehouse_backup": False}


def verify(archive_path, expected_zip_sha256):
    path = Path(archive_path).absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)) and path.is_file()
            and path.stat().st_nlink == 1 and 0 < path.stat().st_size <= MAX_TOTAL, "ARCHIVE_REQUIRED")
    require(re.fullmatch(r"[a-f0-9]{64}", expected_zip_sha256 or ""), "EXTERNAL_PIN_REQUIRED")
    with path.open("rb") as stream:
        require(hashlib.file_digest(stream, "sha256").hexdigest() == expected_zip_sha256, "ZIP_PIN_CHANGED")
    with zipfile.ZipFile(path) as archive:
        entries = archive.infolist(); names = [safe_name(item.filename) for item in entries]
        require(0 < len(names) <= 1024 and len(names) == len(set(names))
                and sum(item.file_size for item in entries) <= MAX_TOTAL, "ARCHIVE_BOUND_OR_DUPLICATES")
        for item in entries:
            require(not item.is_dir() and not item.flag_bits & 1 and not stat.S_ISLNK(item.external_attr >> 16)
                    and 0 <= item.file_size <= MAX_FILE, "UNSAFE_ZIP_MEMBER")
        require("manifest.json" in names and archive.getinfo("manifest.json").file_size <= 2 * 1024**2,
                "MANIFEST_REQUIRED")
        manifest = strict_json(archive.read("manifest.json"))
        require(manifest.get("version") == VERSION and manifest.get("artifact_kind") == "PARTIAL_PROCESSED_CATALOG"
                and manifest.get("research_ready") is False and manifest.get("official_exported") is False
                and manifest.get("warehouse_backup") is False
                and manifest.get("contains_gold_tables") is False
                and manifest.get("allowed_usage") == "EXPLORATORY_ONLY", "PARTIAL_SCOPE_REQUIRED")
        files = manifest["files"]
        require(set(names) == set(files) | {"manifest.json"}, "EXACT_FILE_SET_REQUIRED")
        parquet = {name for name, item in files.items() if item.get("dataset") in KINDS}
        listed = [name for batch in manifest["batches"] for name in batch["files"]]
        require(len(listed) == len(set(listed)) and set(listed) == parquet, "EXACT_PARQUET_ASSIGNMENT_REQUIRED")
        covered = [month for batch in manifest["batches"] for month in batch["affected_months"]]
        require(len(covered) == len(set(covered)) and set(covered) == set(manifest["canonical_by_month"]),
                "DISJOINT_MONTHS_REQUIRED")
        for name, meta in files.items():
            raw = archive.read(name)
            require(len(raw) == meta["bytes"] and sha(raw) == meta["sha256"], "MEMBER_HASH_CHANGED")
        return {**semantic(manifest, archive.read), "zip_sha256": expected_zip_sha256,
                "manifest_sha256": sha(archive.read("manifest.json")), "files": len(names)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--zip-sha256", required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.archive, args.zip_sha256), indent=2, sort_keys=True))
