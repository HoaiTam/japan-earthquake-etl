"""DAT-03 offline collection loader: verify all bytes, read ONE whole-window shard at a time.

Expected manifest/checksum digests must come from the external release receipt.
No ETL, feature generation, sampling, fitting, network, credentials, or pipeline writes.
"""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import platform
import re

import pyarrow.parquet as pq
from experiment_harness import read_json_safe, require, sha256_bytes, MODE_OFFICIAL
from pilot_bundle import FEATURE_TYPES, MAIN_TYPES, typed_schema

VERSION = "dat03-collection-export-v1"
MAX_FILE = 64 * 1024**2


def _safe(path):
    p = Path(path).absolute()
    require(not any(q.is_symlink() for q in (p, *p.parents)), "COLLECTION_SYMLINK_REJECTED")
    require(p == p.resolve(), "COLLECTION_UNSAFE_PATH")
    return p


def _hash(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _index(root, name):
    path = _safe(root / name)
    require(path.is_file() and path.stat().st_size <= 32768, "COLLECTION_INDEX_INVALID")
    result = {}
    for line in path.read_text(encoding="ascii").splitlines():
        require(re.fullmatch(r"[a-f0-9]{64}  (?:dataset_manifest\.json|collection-preparation\.json|export-checksums\.sha256|shard_[0-9]{5}/(?:features|mainshocks)\.parquet)", line) is not None,
                "COLLECTION_INDEX_INVALID")
        digest, relative = line.split("  ")
        require(relative not in result, "COLLECTION_DUPLICATE_INDEX")
        file = _safe(root / relative)
        require(file.is_file() and 0 < file.stat().st_size <= MAX_FILE and _hash(file) == digest,
                "COLLECTION_CHECKSUM_MISMATCH:" + relative)
        result[relative] = digest
    return result


class ResearchCollection:
    def __init__(self, directory, *, manifest_sha256, checksums_sha256):
        self.root = _safe(directory)
        require(self.root.is_dir(), "COLLECTION_DIRECTORY_REQUIRED")
        require(all(isinstance(pin, str) and re.fullmatch(r"[a-f0-9]{64}", pin) for pin in (manifest_sha256, checksums_sha256)),
                "COLLECTION_EXTERNAL_PINS_REQUIRED")
        for name, pin in (("dataset_manifest.json", manifest_sha256), ("checksums.sha256", checksums_sha256)):
            p = _safe(self.root / name)
            require(p.is_file() and p.stat().st_size <= 4*1024**2 and _hash(p) == pin, "COLLECTION_EXTERNAL_PIN_MISMATCH")
        self.manifest_sha256, self.checksums_sha256 = manifest_sha256, checksums_sha256
        self.index = _index(self.root, "checksums.sha256")
        self.manifest = read_json_safe((self.root / "dataset_manifest.json").read_bytes())
        m = self.manifest
        require(m.get("export_version") == VERSION and m.get("dataset_status") == "EXPORTED"
                and m.get("schema_version") == "1.0", "COLLECTION_OFFICIAL_EXPORTED_REQUIRED")
        require(re.fullmatch(r"ds_[a-f0-9]{64}", m.get("dataset_id", "")) is not None, "COLLECTION_DATASET_ID_INVALID")
        self.plan = m["collection_plan"]
        p = self.plan
        require(p.get("dataset_id") == m["dataset_id"] and p.get("collection_id") == "col_" + p.get("plan_sha256", "")
                and re.fullmatch(r"col_[a-f0-9]{64}", p.get("collection_id", "")) is not None, "COLLECTION_PLAN_INVALID")
        prepared = read_json_safe((self.root / "collection-preparation.json").read_bytes())
        require(prepared.get("status") == "RESEARCH_COLLECTION_PREPARED" and prepared.get("plan") == p,
                "COLLECTION_PREPARATION_CHANGED")
        require(p.get("full_mainshock_assignment_verified") is True and p.get("split_windows") is False,
                "COLLECTION_FULL_ASSIGNMENT_REQUIRED")
        self.shards = {s["shard_id"]: s for s in p["shards"]}
        require(1 <= len(p["shards"]) == len(self.shards) <= 64 and list(self.shards) == [f"shard_{i:05d}" for i in range(len(self.shards))],
                "COLLECTION_SHARD_SET_INVALID")
        assigned = {}
        rows = 0
        for s in self.shards.values():
            require(type(s["mainshock_count"]) is int and 1 <= s["mainshock_count"] == len(s["mainshocks"]) <= 128,
                    "COLLECTION_SHARD_RESOURCE_LIMIT")
            count = 0
            for entry in s["mainshocks"]:
                mid, n = entry["mainshock_event_id"], entry["candidate_row_count"]
                require(isinstance(mid, str) and 0 < len(mid) <= 256 and mid not in assigned and type(n) is int and n > 0,
                        "COLLECTION_ASSIGNMENT_INVALID")
                assigned[mid] = s["shard_id"]
                count += n
            require(type(s["feature_row_count"]) is int and 1 <= count == s["feature_row_count"] <= 100000,
                    "COLLECTION_SHARD_RESOURCE_LIMIT")
            rows += count
        require(all(type(value) is int for value in (p["feature_row_count"],m["feature_row_count"],p["mainshock_count"],m["mainshock_count"]))
                and rows == p["feature_row_count"] == m["feature_row_count"] and len(assigned) == p["mainshock_count"] == m["mainshock_count"] <= 4096,
                "COLLECTION_TOTALS_UNBALANCED")
        require(p["global_mc"].get("scope") == "FULL_PINNED_SPLIT" and type(p["global_mc"]["mc_value"]) in (int,float)
                and math.isfinite(p["global_mc"]["mc_value"]),
                "COLLECTION_GLOBAL_MC_REQUIRED")
        require(m.get("dataset_split") in ("REPRODUCTION","EXTENSION")
                and p["coverage"].get("status") == "COMPLETE" and p["coverage"].get("required_month_count") ==
                {"REPRODUCTION": 225, "EXTENSION": 63}.get(m.get("dataset_split")), "COLLECTION_COMPLETE_SPLIT_REQUIRED")
        expected = {"dataset_manifest.json", "collection-preparation.json", "export-checksums.sha256"}
        for name in self.shards:
            expected.update({name + "/features.parquet", name + "/mainshocks.parquet"})
        found = set()
        for item in self.root.rglob("*"):
            item = _safe(item)
            relative = item.relative_to(self.root).as_posix()
            if item.is_dir():
                require(relative in self.shards, "COLLECTION_EXTRA_DIRECTORY")
            else:
                require(item.is_file(), "COLLECTION_UNSAFE_FILE")
                found.add(relative)
        require(found == expected | {"checksums.sha256"} and set(self.index) == expected, "COLLECTION_FILE_SET_CHANGED")
        data = _index(self.root, "export-checksums.sha256")
        require(data == {k: v for k, v in self.index.items() if k not in ("dataset_manifest.json", "export-checksums.sha256")}
                and _hash(self.root / "export-checksums.sha256") == m.get("export_manifest_sha256"), "COLLECTION_DATA_INDEX_CHANGED")

    def load_shard(self, shard_id):
        require(shard_id in self.shards, "COLLECTION_UNKNOWN_SHARD")
        s = self.shards[shard_id]
        files = {}
        for file, count in (("features.parquet", s["feature_row_count"]), ("mainshocks.parquet", s["mainshock_count"])):
            path = _safe(self.root / shard_id / file)
            require(_hash(path) == self.index[shard_id + "/" + file], "COLLECTION_CHECKSUM_MISMATCH")
            parquet = pq.ParquetFile(path)
            require(parquet.metadata.num_rows == count, "COLLECTION_SHARD_COUNT_CHANGED")
            files[file] = parquet.read()
        features, main = files["features.parquet"], files["mainshocks.parquet"]
        types = {k: v for k, v in FEATURE_TYPES.items() if k != "pilot_id"}
        typed_schema(features, self.manifest["feature_schema"], types)
        main_types={k:v for k,v in MAIN_TYPES.items() if k!="pilot_id"}
        typed_schema(main,[{"name":name,"type":main_types.get(name,"UNKNOWN")} for name in main.column_names],main_types)
        declared = {w["mainshock_event_id"]: w["candidate_row_count"] for w in s["mainshocks"]}
        actual = Counter()
        keys, self_ids = set(), set()
        for row in features.to_pylist():
            require(all(v is not None for v in row.values()), "COLLECTION_NULL_FEATURE")
            require(row["dataset_id"] == self.manifest["dataset_id"] and row["mainshock_event_id"] in declared
                    and row["mc_value"] == self.plan["global_mc"]["mc_value"] and row["schema_version"] == "1.0",
                    "COLLECTION_FEATURE_LINEAGE_CHANGED")
            for key in ("feature_version", "scaling_config_version", "window_model_version"):
                require(row[key] == self.manifest[key], "COLLECTION_FEATURE_VERSION_CHANGED")
            require(all(math.isfinite(v) for v in row.values() if isinstance(v, float)), "COLLECTION_NONFINITE_FEATURE")
            key = (row["mainshock_event_id"], row["candidate_event_id"])
            require(key not in keys, "COLLECTION_DUPLICATE_GRAIN"); keys.add(key)
            actual[key[0]] += 1
            is_self = key[0] == key[1]
            require(row["is_mainshock"] is is_self and row["relative_time_role"] in ("PRE", "MAINSHOCK", "POST")
                    and (row["relative_time_role"] == "MAINSHOCK") == is_self, "COLLECTION_ROLE_CHANGED")
            if is_self:
                self_ids.add(key[0])
                require(all(abs(row[c]) <= 1e-6 for c in ("x_scaled", "y_scaled", "z_scaled", "time_scaled")), "COLLECTION_NONZERO_SELF")
        require(dict(actual) == declared and self_ids == set(declared), "COLLECTION_ASSIGNMENT_CHANGED")
        main_rows = main.to_pylist()
        require(len(main_rows) == len(declared) and {r["mainshock_event_id"]: r["candidate_count"] for r in main_rows} == declared
                and all(r["dataset_id"] == self.manifest["dataset_id"] and all(v is not None for k,v in r.items() if k!="resource_reason_code")
                        and all(math.isfinite(v) for v in r.values() if isinstance(v,float)) and r["schema_version"]=="1.0"
                        and r["mc_value"]==self.plan["global_mc"]["mc_value"]
                        and r["window_model_version"]==self.manifest["window_model_version"] for r in main_rows), "COLLECTION_MAINSHOCK_CHANGED")
        return {"mode": MODE_OFFICIAL, "bundle_id": self.manifest["dataset_id"], "source_dataset_id": self.manifest["dataset_id"],
                "manifest": self.manifest, "features_table": features, "mainshocks_table": main, "coverage": self.plan["coverage"],
                "bundle_dir": self.root, "is_official": True, "checksums": self.index,
                "collection_scope": {"version": VERSION, "collection_id": self.plan["collection_id"], "shard_id": shard_id,
                                     "scope": "WHOLE_WINDOW_SHARD"}}

    def verify_all(self):
        count = 0
        for name in self.shards:
            loaded = self.load_shard(name)
            count += loaded["features_table"].num_rows
            del loaded
        return {"status": "COLLECTION_VERIFIED", "dataset_id": self.manifest["dataset_id"], "collection_id": self.plan["collection_id"],
                "manifest_sha256": self.manifest_sha256, "checksums_sha256": self.checksums_sha256,
                "shard_count": len(self.shards), "feature_row_count": count, "mainshock_count": self.plan["mainshock_count"],
                "loader_version": VERSION, "all_shards_verified": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--checksums-sha256", required=True)
    parser.add_argument("--recipient", required=True, choices=("HoaiTam", "ThanhTris", "rosy179"))
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    receipt = _safe(args.receipt)
    collection = ResearchCollection(args.bundle, manifest_sha256=args.manifest_sha256, checksums_sha256=args.checksums_sha256)
    require(not receipt.is_relative_to(collection.root), "COLLECTION_EXTERNAL_RECEIPT_REQUIRED")
    result = collection.verify_all()
    result.update(recipient=args.recipient, verified_at_utc=datetime.now(timezone.utc).isoformat(),
                  machine_fingerprint_sha256=sha256_bytes(platform.node().encode()), python_version=platform.python_version())
    with receipt.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, sort_keys=True, allow_nan=False)
        stream.write("\n")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
