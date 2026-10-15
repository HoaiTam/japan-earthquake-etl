"""DAT-02 offline inspection only. No feature generation, fitting, APIs or official imports."""
import argparse
from collections import Counter
from datetime import datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import re

import pyarrow as pa
import pyarrow.parquet as pq

VECTOR = ["x_scaled", "y_scaled", "z_scaled", "time_scaled"]
FEATURE_TYPES = {
    **dict.fromkeys(["schema_version", "dataset_id", "mainshock_event_id", "candidate_event_id", "relative_time_role",
                     "window_model_version", "feature_version", "scaling_config_version", "pilot_id"], "string"),
    **dict.fromkeys(["candidate_latitude", "candidate_longitude", "candidate_depth_km", "candidate_magnitude", "mainshock_magnitude",
                     "dx_km", "dy_km", "dz_km", "delta_time_hours", "distance_3d_km", "mc_value", *VECTOR,
                     "space_scale_km", "depth_scale_km", "time_scale_hours"], "double"),
    "is_mainshock": "boolean", "candidate_time_utc": "timestamp", "created_at_utc": "timestamp",
}
MAIN_TYPES = {
    **dict.fromkeys(["schema_version", "dataset_id", "mainshock_event_id", "selection_rule_version", "window_model_version", "resource_guard_status", "resource_reason_code", "pilot_id"], "string"),
    **dict.fromkeys(["latitude", "longitude", "depth_km", "magnitude", "mc_value", "pre_window_hours", "post_window_hours", "search_radius_km"], "double"),
    "event_time_utc": "timestamp", "created_at_utc": "timestamp", "candidate_count": "long",
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def read_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "PILOT_DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    return json.loads(data, object_pairs_hook=pairs, parse_constant=lambda _: require(False, "PILOT_NONFINITE_JSON"))


def typed_schema(table, declared, expected):
    names = [entry["name"] for entry in declared]
    require(len(set(names)) == len(names) and names == table.column_names, "PILOT_SCHEMA_ORDER_MISMATCH")
    require({entry["name"]: entry["type"] for entry in declared} == expected, "PILOT_SCHEMA_CONTRACT_MISMATCH")
    arrow_types = {"string": pa.string(), "double": pa.float64(), "boolean": pa.bool_(), "long": pa.int64(), "timestamp": pa.timestamp("us", tz="UTC")}
    for name in names:
        require(table.schema.field(name).type == arrow_types[expected[name]], "PILOT_SCHEMA_TYPE_MISMATCH:" + name)


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.utcoffset().total_seconds() == 0, "PILOT_UTC_REQUIRED")
    return parsed


def load_bundle(directory):
    root = Path(directory)
    require(root.is_dir() and not root.is_symlink(), "PILOT_EXPLICIT_LOCAL_DIRECTORY_REQUIRED")
    names = {"features-part-00000.parquet", "mainshocks.parquet", "pilot_manifest.json", "coverage_report.json", "checksums.sha256"}
    require({p.name for p in root.iterdir()} == names, "PILOT_EXACT_FILE_SET_REQUIRED")
    for name in names:
        path = root / name
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 16_777_216, "PILOT_UNSAFE_OR_OVERSIZED_FILE")
    checksum_entries = {}
    for line in (root / "checksums.sha256").read_text().splitlines():
        require(re.fullmatch(r"[a-f0-9]{64}  [A-Za-z0-9_.-]+", line) is not None, "PILOT_CHECKSUM_FORMAT")
        digest, name = line.split("  ")
        require(name not in checksum_entries, "PILOT_DUPLICATE_CHECKSUM")
        checksum_entries[name] = digest
    require(set(checksum_entries) == names - {"checksums.sha256"}, "PILOT_CHECKSUM_FILE_SET")
    for name, digest in checksum_entries.items():
        require(sha((root / name).read_bytes()) == digest, "PILOT_CHECKSUM_MISMATCH:" + name)
    manifest = read_json((root / "pilot_manifest.json").read_bytes())
    coverage = read_json((root / "coverage_report.json").read_bytes())
    require(manifest["manifest_version"] == "dat-02-pilot-v1" and manifest["artifact_kind"] == "DEVELOPMENT_PILOT"
            and manifest["allowed_usage"] == "DEVELOPMENT_ONLY" and manifest["research_ready"] is False
            and manifest["source_dataset_status"] == "BUILDING", "PILOT_DEVELOPMENT_ONLY_REQUIRED")
    payload = manifest["identity_payload"]
    require(manifest["pilot_id"] == "pilot_" + sha(canonical(payload).encode()), "PILOT_IDENTITY_MISMATCH")
    require(payload["source_dataset_id"] == manifest["source_dataset_id"], "PILOT_SOURCE_ID_MISMATCH")
    for name in ["scaling", "selection"]:
        require(sha(manifest[name + "_config_json"].encode()) == payload[name + "_config_sha256"], "PILOT_CONFIG_HASH_MISMATCH")
    require(manifest["vector_columns"] == VECTOR and manifest["feature_version"] == "relative-4d-v1", "PILOT_VECTOR_CONTRACT")
    require(manifest["schema_sha256"] == sha(canonical(manifest["feature_schema"]).encode()), "PILOT_SCHEMA_HASH_MISMATCH")
    require(manifest["coverage_sha256"] == checksum_entries["coverage_report.json"], "PILOT_COVERAGE_HASH_MISMATCH")
    source = manifest["source_manifest"]
    publication = manifest["gold_publication"]
    require(source["dataset_id"] == manifest["source_dataset_id"] and source["dataset_status"] == "BUILDING"
            and source["gold_publication_id"] == publication["publication_id"] and publication["published"] is True
            and publication["verify_status"] == "PASSED", "PILOT_PUBLISHED_LINEAGE_REQUIRED")
    pins = publication["snapshot_bundle"]
    require(set(pins) == {"event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band"}, "PILOT_GOLD_BUNDLE_INCOMPLETE")
    require(all(type(pin["snapshot_id"]) is int and pin["snapshot_id"] > 0 for pin in pins.values()), "PILOT_EXACT_64BIT_SNAPSHOT_REQUIRED")
    require(source["gold_snapshot_id"] == pins["event_current"]["snapshot_id"]
            and source["physical_gold_table_name"] == pins["event_current"]["table"], "PILOT_GOLD_PIN_MISMATCH")
    selection = read_json(manifest["selection_config_json"])
    require(selection["native_report_sha256"] == payload["native_report_sha256"]
            and selection["source_dataset_id"] == manifest["source_dataset_id"]
            and sorted(selection["mainshock_ids"]) == manifest["selected_mainshock_ids"], "PILOT_SELECTION_PIN_MISMATCH")
    # Read exact files, not a Hive dataset inferred from pilot_id=<id> parent directories.
    feature_file = pq.ParquetFile(root / "features-part-00000.parquet")
    main_file = pq.ParquetFile(root / "mainshocks.parquet")
    require(1 <= main_file.metadata.num_rows <= 3 and 1 <= feature_file.metadata.num_rows <= 30000, "PILOT_RESOURCE_LIMIT")
    features, mainshocks = feature_file.read(), main_file.read()
    typed_schema(features, manifest["feature_schema"], FEATURE_TYPES)
    typed_schema(mainshocks, manifest["mainshock_schema"], MAIN_TYPES)
    require(1 <= mainshocks.num_rows <= 3 and features.num_rows <= 30000 and manifest["invalid_count"] == 0,
            "PILOT_RESOURCE_OR_INVALID_COUNT")
    require(features.num_rows == manifest["feature_count"] and mainshocks.num_rows == manifest["mainshock_count"], "PILOT_COUNT_MISMATCH")
    rows, mains = features.to_pylist(), mainshocks.to_pylist()
    ids = manifest["selected_mainshock_ids"]
    require(len(set(ids)) == len(ids) == len(mains), "PILOT_MAINSHOCK_GRAIN")
    require({r["mainshock_event_id"] for r in mains} == set(ids), "PILOT_MAINSHOCK_SELECTION_MISMATCH")
    require(len({(r["dataset_id"], r["mainshock_event_id"], r["candidate_event_id"]) for r in rows}) == len(rows), "PILOT_DUPLICATE_GRAIN")
    counts, self_counts = Counter(), Counter()
    main_by_id = {r["mainshock_event_id"]: r for r in mains}
    scaling = read_json(manifest["scaling_config_json"])
    require(scaling["mode"] in {"GLOBAL", "ADAPTIVE_WINDOW"} and scaling["depth_scale_km"] > 0, "PILOT_SCALING_CONFIG")
    for row in rows + mains:
        require(all(value is not None for key, value in row.items() if key != "resource_reason_code"), "PILOT_NULL_FIELD")
        require(row["pilot_id"] == manifest["pilot_id"] and row["dataset_id"] == manifest["source_dataset_id"], "PILOT_ROW_LINEAGE_MISMATCH")
        for value in row.values():
            require(not isinstance(value, float) or math.isfinite(value), "PILOT_NONFINITE_VALUE")
    for row in rows:
        mid = row["mainshock_event_id"]
        require(mid in ids and row["feature_version"] == manifest["feature_version"]
                and row["scaling_config_version"] == manifest["scaling_config_version"], "PILOT_FEATURE_LINEAGE_MISMATCH")
        main = main_by_id[mid]
        require(row["mainshock_magnitude"] == main["magnitude"] and row["mc_value"] == main["mc_value"]
                and row["window_model_version"] == main["window_model_version"]
                and row["candidate_magnitude"] >= main["mc_value"], "PILOT_CANDIDATE_LINEAGE_MISMATCH")
        require(all(row[k] > 0 for k in ["space_scale_km", "depth_scale_km", "time_scale_hours"]), "PILOT_INVALID_SCALE")
        require(row["depth_scale_km"] == scaling["depth_scale_km"], "PILOT_SCALE_METADATA_MISMATCH")
        for field, source_field in [("space_scale_km", "search_radius_km"), ("time_scale_hours", "post_window_hours")]:
            require(row[field] == (scaling[field] if scaling["mode"] == "GLOBAL" else main[source_field]), "PILOT_SCALE_METADATA_MISMATCH")
        is_self = row["candidate_event_id"] == mid
        require(row["is_mainshock"] is is_self, "PILOT_SELF_FLAG_MISMATCH")
        expected_role = "MAINSHOCK" if is_self else "PRE" if row["delta_time_hours"] < 0 else "POST"
        require(row["relative_time_role"] == expected_role, "PILOT_ROLE_MISMATCH")
        require((row["time_scaled"] == 0) == (row["delta_time_hours"] == 0)
                and (row["time_scaled"] < 0) == (row["delta_time_hours"] < 0), "PILOT_TIME_SIGN_MISMATCH")
        if is_self:
            self_counts[mid] += 1
            require(all(row[k] == 0 for k in VECTOR + ["dx_km", "dy_km", "dz_km", "delta_time_hours", "distance_3d_km"]), "PILOT_NONZERO_SELF")
            require(row["candidate_time_utc"] == main["event_time_utc"]
                    and all(row["candidate_" + key] == main[key] for key in ["latitude", "longitude", "depth_km", "magnitude"]), "PILOT_SELF_SOURCE_MISMATCH")
        counts[mid] += 1
    for main in mains:
        mid = main["mainshock_event_id"]
        require(50 <= main["depth_km"] <= 200 and main["magnitude"] > 5.5, "PILOT_MAINSHOCK_RULE")
        require(counts[mid] == main["candidate_count"] and self_counts[mid] == 1, "PILOT_MISSING_SELF_OR_COUNT")
        require(main["resource_guard_status"] == "PASS", "PILOT_RESOURCE_GUARD_FAILED")
    require(coverage["research_ready"] is False and len(coverage["windows"]) == len(ids)
            and {w["mainshock_event_id"] for w in coverage["windows"]} == set(ids), "PILOT_COVERAGE_GRAIN")
    for window in coverage["windows"]:
        require(window["candidate_count"] == counts[window["mainshock_event_id"]], "PILOT_COVERAGE_COUNT")
        a, b = utc(window["required_start_utc"]), utc(window["required_end_utc_exclusive"])
        require(a < b, "PILOT_COVERAGE_WINDOW")
        main = main_by_id[window["mainshock_event_id"]]
        require(abs((a - (main["event_time_utc"] - timedelta(hours=main["pre_window_hours"]))).total_seconds()) <= 1e-6
                and abs((b - (main["event_time_utc"] + timedelta(hours=main["post_window_hours"], microseconds=1))).total_seconds()) <= 1e-6,
                "PILOT_COVERAGE_WINDOW_TRUNCATED")
        intervals = []
        for kind in ["available_ranges", "missing_ranges"]:
            for interval in window[kind]:
                x, y = utc(interval["start_utc"]), utc(interval["end_utc_exclusive"])
                require(a <= x < y <= b, "PILOT_COVERAGE_RANGE")
                intervals.append((x, y))
        cursor = a
        for x, y in sorted(intervals):
            require(x == cursor, "PILOT_COVERAGE_GAP_OR_OVERLAP")
            cursor = y
        require(cursor == b and window["status"] == ("PARTIAL" if window["missing_ranges"] else "COMPLETE"), "PILOT_COVERAGE_STATUS")
    expected = "PARTIAL" if any(w["status"] == "PARTIAL" for w in coverage["windows"]) else "COMPLETE"
    require(coverage["status"] == manifest["coverage_status"] == expected, "PILOT_COVERAGE_STATUS")
    return {"manifest": manifest, "coverage": coverage, "features": features, "mainshocks": mainshocks,
            "summary": {"status": "PILOT_INSPECTION_PASSED", "pilot_id": manifest["pilot_id"], "features": len(rows),
                        "mainshocks": len(mains), "self_vectors": dict(self_counts), "coverage_status": expected,
                        "research_ready": False, "allowed_usage": "DEVELOPMENT_ONLY"}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args()
    print(json.dumps(load_bundle(args.bundle)["summary"], indent=2))
