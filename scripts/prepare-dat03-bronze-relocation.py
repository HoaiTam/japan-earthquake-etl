#!/usr/bin/env python3
"""Prepare pinned metadata for Java original-to-physical Bronze readback; no lake writes."""
import argparse
import base64
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location("followup", Path(__file__).with_name("verify-dat03-followup.py"))
f = importlib.util.module_from_spec(spec); spec.loader.exec_module(f)
a = f.lineage


def physical_pins(config):
    root = Path(config["catalog"])
    request = f.pinned_file(root / "request.json", config["catalog_request_sha256"], 1024**2)
    observation = f.pinned_file(config["observation_manifest"], config["observation_manifest_sha256"], 8 * 1024**2)
    manifest = a.decode(observation.read_bytes())
    a.require(manifest["catalog_request_sha256"] == f.file_sha(request), "CATALOG_REQUEST_CHANGED")
    metadata = manifest["files"]["bronze-pins.json"]
    a.require(metadata["sha256"] == config["bronze_pins_sha256"] and type(metadata["bytes"]) is int
              and 0 < metadata["bytes"] <= 1024**2, "BRONZE_PIN_DESCRIPTOR_CHANGED")
    blob = root / "host-observations-v1" / "blobs" / metadata["sha256"][:2] / metadata["sha256"]
    pins_path = f.pinned_file(blob, metadata["sha256"], 1024**2)
    a.require(pins_path.stat().st_size == metadata["bytes"], "BRONZE_PIN_SIZE_CHANGED")
    pins = a.decode(pins_path.read_bytes())
    a.require(isinstance(pins, list) and 0 < len(pins) <= 64
              and len({p["manifest_uri"] for p in pins}) == len(pins), "DISTINCT_PHYSICAL_PINS_REQUIRED")
    return pins


def worker_sources(worker):
    plan_path = f.pinned_file(worker["followup_plan"], worker["followup_plan_sha256"], 1024**2)
    plan = a.decode(plan_path.read_bytes()); receipt = f.verify(plan)
    a.require(receipt["owner"] == worker["owner"], "FOLLOWUP_OWNER_MISMATCH")
    roots_path = f.pinned_file(worker["roots"], worker["roots_sha256"], 1024**2)
    roots = a.decode(roots_path.read_bytes())
    objects, _ = a.load_archives([f.pinned_file(x["path"], x["sha256"], 8 * 1024**2) for x in plan["metadata_archives"]])
    records, seen = [], set()
    for item in roots["objects"]:
        key = a.key(item["uri"])
        if not key.startswith("bronze/"): continue
        a.require(key.endswith("/manifest.json") and key not in seen, "DISTINCT_BRONZE_ROOT_REQUIRED"); seen.add(key)
        a.require(key in objects and a.digest(objects[key]) == item["sha256"], "WORKER_BRONZE_ROOT_PIN_MISMATCH")
        records.append({"owner": worker["owner"], "manifest_uri": item["uri"], "manifest_sha256": item["sha256"],
                        "manifest_base64": base64.b64encode(objects[key]).decode("ascii")})
    a.require(0 < len(records) <= 64, "BOUNDED_WORKER_SOURCES_REQUIRED")
    return records


def prepare(config):
    a.require(config.get("version") == "dat03-bronze-relocation-preparation-v1", "PREPARATION_VERSION_REQUIRED")
    pins = physical_pins(config); workers = config["workers"]
    a.require(0 < len(workers) <= 2 and len({w["owner"] for w in workers}) == len(workers)
              and all(w["owner"] in ("ThanhTris", "rosy179") for w in workers), "DISTINCT_APPROVED_WORKERS_REQUIRED")
    originals = []
    for worker in workers:
        sources = worker_sources(worker)
        a.require(len(sources) == len(pins), "COMPLETE_WORKER_SOURCE_SET_REQUIRED"); originals.extend(sources)
    return {"version": "dat03-bronze-relocation-plan-v1", "physical_bronze_inputs": pins, "original_sources": originals,
            "input_pins": {k: config[k] for k in ("catalog_request_sha256", "observation_manifest_sha256", "bronze_pins_sha256")},
            "worker_input_pins": [{k: w[k] for k in ("owner", "followup_plan_sha256", "roots_sha256")} for w in workers],
            "mode": "READ_ONLY_ORIGINAL_TO_PHYSICAL_MAPPING_NO_KEY_RESTORATION"}


def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("--config", type=Path, required=True)
    p.add_argument("--config-sha256", required=True); p.add_argument("--output", type=Path, required=True)
    args = p.parse_args(); path = f.pinned_file(args.config, args.config_sha256, 1024**2)
    result = prepare(a.decode(path.read_bytes())); result["preparation_config_sha256"] = args.config_sha256
    a.require(not args.output.exists() and not args.output.is_symlink(), "NEW_PLAN_PATH_REQUIRED")
    with args.output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, indent=2, allow_nan=False); stream.write("\n")
    print(json.dumps({"plan_sha256": f.file_sha(args.output), "physical_sources": len(result["physical_bronze_inputs"]),
                      "original_sources": len(result["original_sources"]), "lake_writes": False}))


if __name__ == "__main__": main()
