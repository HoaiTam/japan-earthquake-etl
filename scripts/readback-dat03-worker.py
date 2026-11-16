#!/usr/bin/env python3
"""Sequential read-only Java payload gates, <=12 months/batch; immutable checkpoints."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

spec = importlib.util.spec_from_file_location("portable", Path(__file__).with_name("plan-dat03-portable-silver.py"))
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)
a = p.a


def save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    return p.f.file_sha(path)


def months_between(start, end):
    from datetime import date
    date.fromisoformat(start + "-01"); date.fromisoformat(end + "-01")
    y, m = map(int, start.split("-")); result = []
    while f"{y:04d}-{m:02d}" <= end:
        result.append(f"{y:04d}-{m:02d}"); m += 1
        if m == 13: y += 1; m = 1
        a.require(len(result) <= 480, "RANGE_LIMIT")
    a.require(result, "NONEMPTY_RANGE_REQUIRED")
    return result


def partition(config, expected_months):
    followup_path = p.f.pinned_file(config["followup_plan"], config["followup_plan_sha256"], 1024**2)
    followup = a.decode(followup_path.read_bytes())
    roots_path = p.f.pinned_file(config["roots"], config["roots_sha256"], 1024**2)
    roots = a.decode(roots_path.read_bytes())
    objects, _ = a.load_archives([p.f.pinned_file(x["path"], x["sha256"], 8 * 1024**2) for x in followup["metadata_archives"]])
    groups = []
    for item in roots["objects"]:
        key = a.key(item["uri"])
        if key.startswith("silver/bundles/") and key.count("/") == 3 and key.endswith("/manifest.json"):
            raw = objects[key]; a.require(a.digest(raw) == item["sha256"], "BUNDLE_ROOT_PIN_MISMATCH")
            bundle = a.decode(raw); months = bundle["context"]["affected_months"]
            a.require(months and len(months) <= 12, "SOURCE_GROUP_TOO_WIDE")
            groups.append((min(months), bundle["run_id"], months))
    groups.sort()
    actual = [month for _, _, months in groups for month in months]
    a.require(len(set(actual)) == len(actual) and sorted(actual) == expected_months, "EXACT_COVERAGE_REQUIRED")
    batches, current, current_months = [], [], []
    for _, run, months in groups:
        if current and (len(current) == 4 or len(current_months) + len(months) > 12):
            batches.append((current, sorted(current_months))); current, current_months = [], []
        current.append(run); current_months.extend(months)
    if current: batches.append((current, sorted(current_months)))
    return batches


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--config-sha256", required=True)
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--classpath-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    config_path = p.f.pinned_file(args.config, args.config_sha256, 1024**2)
    config = a.decode(config_path.read_bytes())
    expected = months_between(args.start_month, args.end_month)
    batches = partition(config, expected)
    a.require(not args.output.exists() and not args.output.is_symlink(), "NEW_OUTPUT_DIRECTORY_REQUIRED")
    args.output.mkdir()
    started = time.monotonic()
    save(args.output / "started.json", {"config_sha256": args.config_sha256, "pid": os.getpid(), "expected_months": expected, "batch_count": len(batches), "mode": "READ_ONLY_NO_GOLD_WRITES"})
    classpath = "spark/target/classes:" + args.classpath_file.read_text().strip()
    accepted, previous, total_rows, total_bytes = [], None, 0, 0
    try:
        for ordinal, (runs, months) in enumerate(batches, 1):
            a.require(time.monotonic() - started < 6900, "TWO_HOUR_SESSION_BOUND")
            a.require(shutil.disk_usage(args.output).free >= 4 * 1024**3, "FOUR_GIB_DISK_RESERVE")
            batch_config = {**config, "run_ids": runs}
            plan = p.prepare(batch_config)
            plan_path = args.output / f"batch-{ordinal:02d}-plan.json"
            plan_sha = save(plan_path, plan)
            receipt_path = args.output / f"batch-{ordinal:02d}-readback.json"
            print(f"event=portable_readback_batch_started batch={ordinal}/{len(batches)} months={months[0]}..{months[-1]}", flush=True)
            result = subprocess.run(["java", "-Xmx1536m", "-cp", classpath, "ie212.earthquake.spark.silver.PortableSilverReadback", str(plan_path), plan_sha, str(receipt_path)], capture_output=True, text=True, timeout=min(1500, int(6900 - (time.monotonic() - started))))
            if result.returncode != 0:
                # Never retry or accept a possibly half-finished run. Keep the exact local stderr.
                with (args.output / f"batch-{ordinal:02d}-failure.log").open("x") as stream: stream.write(result.stderr)
                raise RuntimeError("JAVA_READBACK_FAILED")
            receipt = a.decode(receipt_path.read_bytes())
            a.require(receipt["plan_sha256"] == plan_sha and len(receipt["groups"]) == len(runs), "RECEIPT_PLAN_MISMATCH")
            a.require(receipt["gold_published"] is False and receipt["portable_import_ready"] is False, "READ_ONLY_FLAGS_REQUIRED")
            accepted.extend(months); total_rows += receipt["rows_decoded"]; total_bytes += receipt["bytes_checked"]
            previous = save(args.output / f"batch-{ordinal:02d}-checkpoint.json", {"version": "dat03-readback-checkpoint-v1", "previous_checkpoint_sha256": previous, "config_sha256": args.config_sha256, "plan_sha256": plan_sha, "receipt_sha256": p.f.file_sha(receipt_path), "batch": ordinal, "accepted_readback_months": accepted, "rows_decoded": total_rows, "bytes_checked": total_bytes, "gold_published": False})
            print(f"event=portable_readback_batch_accepted batch={ordinal}/{len(batches)} months_verified={len(accepted)}/{len(expected)} rows_decoded={total_rows} gold_published=false", flush=True)
        a.require(sorted(accepted) == expected and len(set(accepted)) == len(expected), "FINAL_COVERAGE_MISMATCH")
        save(args.output / "completed.json", {"version": "dat03-readback-completed-v1", "config_sha256": args.config_sha256, "last_checkpoint_sha256": previous, "months_verified": len(accepted), "batch_count": len(batches), "rows_decoded": total_rows, "bytes_checked": total_bytes, "elapsed_seconds": round(time.monotonic() - started, 3), "gold_published": False, "portable_import_ready": False, "official_export_ready": False})
    except Exception as exc:
        save(args.output / "failed.json", {"error_type": type(exc).__name__, "accepted_readback_months": accepted, "last_checkpoint_sha256": previous, "automatic_retry": False, "gold_published": False})
        raise


if __name__ == "__main__": main()
