#!/usr/bin/env python3
"""Bounded offline metadata frontier audit, not ETL or Gold acceptance."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile

SHA = re.compile(r"[0-9a-f]{64}\Z")
PREFIX = "s3://japan-earthquake/"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def require(ok, code):
    if not ok:
        raise ValueError(code)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "DUPLICATE_JSON_KEY")
        result[key] = value
    return result


def decode(data):
    return json.loads(data, object_pairs_hook=pairs,
                      parse_constant=lambda _: require(False, "NONFINITE_JSON"))


def key(uri):
    require(isinstance(uri, str) and uri.startswith(PREFIX), "FOREIGN_OR_INVALID_URI")
    value = uri[len(PREFIX):]
    require(value and not value.startswith("/") and "\\" not in value
            and ":" not in value and "\0" not in value
            and all(p not in ("", ".", "..") for p in value.split("/")), "UNSAFE_KEY")
    return value


def load_archives(paths):
    objects, inventories = {}, []
    require(0 < len(paths) <= 8, "ARCHIVE_COUNT_LIMIT")
    total = 0
    for path in paths:
        path = Path(path)
        require(not path.is_symlink() and path.stat().st_size <= 8 * 1024**2, "ARCHIVE_LIMIT")
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            require(len(entries) <= 512, "ENTRY_LIMIT")
            seen = set()
            for info in entries:
                name = key(PREFIX + info.filename)
                require(name not in seen, "DUPLICATE_ZIP_ENTRY")
                seen.add(name)
                require(not info.is_dir() and not stat.S_ISLNK(info.external_attr >> 16)
                        and not (info.flag_bits & 1), "UNSAFE_ZIP_ENTRY")
                total += info.file_size
                require(info.file_size <= 2 * 1024**2 and total <= 64 * 1024**2, "DECODED_LIMIT")
                data = archive.read(info)
                # Package-level index/readme are not objects in the source bucket.
                # They may legitimately differ across independent archives.
                if name in {"manifest.json", "README.md"}:
                    continue
                # Trí's handoff wraps bucket keys in metadata/. This is a
                # transport prefix only; expected object SHA still comes from
                # the independently pinned checkpoint, never the ZIP index.
                if name.startswith("metadata/"):
                    name = key(PREFIX + name[len("metadata/"):])
                require(name not in objects or objects[name] == data, "CONFLICTING_OBJECT_BYTES")
                objects[name] = data
            inventories.append({"sha256": digest(path.read_bytes()), "entries": len(entries)})
    return objects, inventories


def audit(roots_path, roots_sha256, archives):
    roots_bytes = Path(roots_path).read_bytes()
    require(len(roots_bytes) <= 1024**2 and digest(roots_bytes) == roots_sha256, "ROOT_PIN_MISMATCH")
    roots = decode(roots_bytes)
    objects, inventories = load_archives(archives)
    frontier, queue, snapshots, unpinned = {}, [], [], set()

    def add(uri, sha, kind, parent, size=None):
        name = key(uri)
        require(isinstance(sha, str) and SHA.fullmatch(sha), "INVALID_SHA")
        if name in frontier:
            require(frontier[name]["sha256"] == sha, "CONFLICTING_REFERENCE_SHA")
            if size is not None and frontier[name].get("byte_size") is not None:
                require(frontier[name]["byte_size"] == size, "CONFLICTING_REFERENCE_SIZE")
            return
        require(len(frontier) < 10000, "FRONTIER_LIMIT")
        frontier[name] = {"uri": uri, "sha256": sha, "kind": kind,
                          "referenced_by": parent, "status": "NOT_SUPPLIED_IN_PACKAGES"}
        if size is not None:
            require(isinstance(size, int) and not isinstance(size, bool) and size >= 0, "INVALID_SIZE")
            frontier[name]["byte_size"] = size
        queue.append(name)

    require(isinstance(roots, dict) and isinstance(roots.get("objects"), list)
            and 0 < len(roots["objects"]) <= 256, "INVALID_ROOTS")
    for item in roots["objects"]:
        add(item["uri"], item["sha256"], item["kind"], "TRUSTED_CHECKPOINT")
    require(len(queue) == len(roots["objects"]), "DUPLICATE_ROOT")
    root_names = set(queue)
    for name in queue:
        record = frontier[name]
        if name not in objects:
            continue
        data = objects[name]
        require(digest(data) == record["sha256"], "OBJECT_SHA_MISMATCH")
        if "byte_size" in record:
            require(len(data) == record["byte_size"], "OBJECT_SIZE_MISMATCH")
        record["status"] = "MATCHED"
        if not name.endswith(".json"):
            continue
        value = decode(data)
        require(isinstance(value, dict), "METADATA_NOT_OBJECT")
        if "datasets" in value:
            require(value.get("bundle_version") == "slv-09-bundle-v1", "UNKNOWN_BUNDLE_VERSION")
            for dataset in value["datasets"].values():
                add(dataset["manifest_uri"], dataset["sha256"], "silver_dataset_manifest", record["uri"])
        if "active_inputs" in value:
            for pin in value["active_inputs"]:
                add(pin["manifest_uri"], pin["manifest_sha256"], "bronze_manifest", record["uri"])
            parent = value.get("parent")
            if parent:
                # registry commit_key is relative to SilverObjectStore, NOT bucket root.
                commit = key(PREFIX + parent["commit_key"])
                require(commit.startswith("gold-commits/"), "UNSUPPORTED_PARENT_KEY")
                add(PREFIX + "silver/" + commit, parent["commit_sha256"], "gold_commit", record["uri"])
                add(PREFIX + "silver/full-month/" + parent["run_id"] + "/registry.json",
                    parent["registry_sha256"], "registry_json", record["uri"])
        if "raw_object_uri" in value:
            add(value["raw_object_uri"], value["sha256"], "bronze_raw", record["uri"], value.get("content_length_bytes"))
        if "dataset" in value and "files" in value:
            for part in value["files"]:
                add(part["uri"], part["sha256"], "silver_payload", record["uri"], part["byte_size"])
        if "commit_version" in value:
            require(value["commit_version"] == "gld-03-v1", "UNKNOWN_COMMIT_VERSION")
            identity = str(PurePosixPath(name).parent / "identity.json")
            add(PREFIX + identity, value["identity_sha256"], "gold_identity", record["uri"])
            run = value["gold_run_id"]
            unpinned.add(PREFIX + "silver/gold-verification/" + run + "/publication.json")
            for table, pin in value["tables"].items():
                snapshots.append({"commit_uri": record["uri"], "table": table,
                                  "snapshot_id": pin["snapshot_id"]})
    result = sorted(frontier.values(), key=lambda r: r["uri"])
    missing = [r for r in result if r["status"] != "MATCHED"]
    return {"version": "dat03-lineage-frontier-v1", "roots_sha256": roots_sha256,
            "archives": inventories, "roots_expected": len(root_names),
            "roots_matched": sum(frontier[n]["status"] == "MATCHED" for n in root_names),
            "counts": dict(Counter((r["kind"] + ":" + r["status"]) for r in result)),
            "objects": result, "next_required_objects": missing,
            "unpinned_publication_uris": sorted(unpinned), "snapshot_references": snapshots,
            "supplied_frontier_complete": not missing and not unpinned,
            "portable_import_ready": False, "official_export_ready": False,
            "full_transitive_closure_verified": False,
            "note": "NOT_SUPPLIED means absent from supplied ZIPs, not absent from worker MinIO. "
                    "Snapshot IDs are references, not portable snapshot files. "
                    "Unrecognized metadata is not claimed as full transitive closure."}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--roots", type=Path, required=True)
    p.add_argument("--roots-sha256", required=True)
    p.add_argument("--archive", type=Path, action="append", required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()
    result = audit(args.roots, args.roots_sha256, args.archive)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps({k: result[k] for k in ["roots_expected", "roots_matched", "counts",
                                          "portable_import_ready", "official_export_ready"]}))
    return 0 if result["supplied_frontier_complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
