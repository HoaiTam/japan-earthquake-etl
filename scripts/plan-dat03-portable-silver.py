#!/usr/bin/env python3
"""Prepare exact metadata/file mappings for Java readback, not data ETL or import."""
import argparse
import base64
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("followup", Path(__file__).with_name("verify-dat03-followup.py"))
f = importlib.util.module_from_spec(spec)
spec.loader.exec_module(f)
a = f.lineage


def prepare(config):
    a.require(config.get("version") == "dat03-portable-preparation-v1", "UNKNOWN_PREPARATION_VERSION")
    metadata_plan = f.pinned_file(config["followup_plan"], config["followup_plan_sha256"], 1024**2)
    followup = a.decode(metadata_plan.read_bytes())
    receipt = f.verify(followup)
    roots_path = f.pinned_file(config["roots"], config["roots_sha256"], 1024**2)
    roots = a.decode(roots_path.read_bytes())
    roots_by_uri = {r["uri"]: r for r in roots["objects"]}
    a.require(len(roots_by_uri) == len(roots["objects"]), "DUPLICATE_ROOT")
    objects, _ = a.load_archives([Path(x["path"]) for x in followup["metadata_archives"]])
    inventory_path = f.pinned_file(config["inventory"], config["inventory_sha256"], 4 * 1024**2)
    inventory = a.decode(inventory_path.read_bytes())
    a.require(inventory.get("version") == "dat03-payload-byte-inventory-v1" and inventory["owner"] == receipt["owner"], "INVENTORY_OWNER_MISMATCH")
    a.require(inventory["payload_bytes_available"] is True and inventory["missing_references"] == 0, "PAYLOAD_INVENTORY_INCOMPLETE")
    indexed = {x["uri"]: x for x in inventory["objects"]}
    archives = {x["sha256"]: x for x in config["payload_archives"]}
    a.require(len(archives) == len(config["payload_archives"]), "DUPLICATE_PAYLOAD_ARCHIVE")
    runs = config["run_ids"]
    a.require(0 < len(runs) <= 4 and len(set(runs)) == len(runs), "ONE_TO_FOUR_DISTINCT_RUNS_REQUIRED")
    groups, months = [], set()
    for run in runs:
        a.require(isinstance(run, str) and run and all(c.isalnum() or c in "_-" for c in run), "SAFE_RUN_REQUIRED")
        key = "silver/bundles/" + run + "/manifest.json"
        uri = a.PREFIX + key
        a.require(uri in roots_by_uri and key in objects, "PINNED_BUNDLE_ROOT_REQUIRED")
        raw = objects[key]
        a.require(a.digest(raw) == roots_by_uri[uri]["sha256"], "BUNDLE_ROOT_PIN_MISMATCH")
        bundle = a.decode(raw)
        context = bundle["context"]
        a.require(context.get("quality_passed") is True and context.get("reconciliation_balanced") is True, "SILVER_QUALITY_BLOCKED")
        a.require("publication_quality" not in context, "OPERATIONAL_INPUT_NOT_RESEARCH")
        months.update(context["affected_months"])
        group = {"run_id": run, "bundle_sha256": a.digest(raw), "bundle_base64": base64.b64encode(raw).decode("ascii"), "datasets": {}}
        for name, ref in bundle["datasets"].items():
            mfraw = objects[a.key(ref["manifest_uri"])]
            a.require(a.digest(mfraw) == ref["sha256"], "DATASET_MANIFEST_PIN_MISMATCH")
            manifest = a.decode(mfraw)
            sources = []
            for part in manifest["files"]:
                inv = indexed.get(part["uri"])
                a.require(inv is not None and inv["sha256"] == part["sha256"] and inv["byte_size"] == part["byte_size"], "INVENTORY_REFERENCE_MISMATCH")
                candidates = [x for x in inv["sources"] if x["archive_sha256"] in archives]
                a.require(candidates, "PAYLOAD_ARCHIVE_MAPPING_REQUIRED")
                source = candidates[0]
                sources.append({"uri": part["uri"], "archive_sha256": source["archive_sha256"], "member": source["member"]})
            group["datasets"][name] = {"manifest_base64": base64.b64encode(mfraw).decode("ascii"), "sources": sources}
        groups.append(group)
    a.require(0 < len(months) <= 12, "MAX_TWELVE_AFFECTED_MONTHS")
    return {"version": "dat03-portable-silver-readback-v1", "owner": receipt["owner"],
            "followup_plan_sha256": config["followup_plan_sha256"], "roots_sha256": config["roots_sha256"],
            "inventory_sha256": config["inventory_sha256"], "affected_months": sorted(months),
            "archives": list(archives.values()), "groups": groups,
            "mode": "READ_ONLY_NO_OBJECT_STORE_OR_GOLD_WRITES"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    path = f.pinned_file(args.config, args.config_sha256, 1024**2)
    result = prepare(a.decode(path.read_bytes()))
    a.require(not args.output.exists() and not args.output.is_symlink(), "OUTPUT_EXISTS")
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps({"groups": len(result["groups"]), "months": result["affected_months"], "plan_sha256": f.file_sha(args.output), "mode": result["mode"]}))


if __name__ == "__main__":
    main()
