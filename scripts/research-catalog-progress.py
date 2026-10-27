#!/usr/bin/env python3
"""Read-only DAT-03 accepted checkpoint report. No Docker, ETL or lease writes."""
import argparse
import importlib.util
import json
from pathlib import Path
import re

SPEC = importlib.util.spec_from_file_location("catalog_progress_support", Path(__file__).with_name("research-catalog.py"))
c = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(c)


def milestones(completed):
    c.e.require(type(completed) is int and 0 <= completed <= 288, "CATALOG_PROGRESS_TYPED_COUNT_REQUIRED")
    return [percent for percent in (25, 50, 75, 100) if completed >= 288 * percent // 100]


def report(catalog, runtime, session_id):
    c.e.require(isinstance(session_id, str) and re.fullmatch(r"[a-f0-9]{32}", session_id), "CATALOG_PROGRESS_EXACT_SESSION_REQUIRED")
    catalog, runtime = map(c.e.safe, (catalog, runtime))
    runtime = c.observation_view(runtime)
    request = c.validate(catalog)
    c.e.require(c.validate(runtime) == request and c.e.sha(catalog / "request.json") == c.e.sha(runtime / "request.json"),
                "CATALOG_PROGRESS_FROZEN_INPUT_CHANGED")
    session = runtime / "sessions" / session_id
    acceptance = c.load(session / "acceptance.json", 65536)
    execution = c.load(session / "execution.json", 65536)
    c.e.require(acceptance["version"] == c.VERSION and acceptance["status"] == "CHECKPOINT_ACCEPTED"
                and acceptance["request_sha256"] == c.e.sha(runtime / "request.json")
                and acceptance["result_sha256"] == c.e.sha(session / "result.json")
                and acceptance["execution_sha256"] == c.e.sha(session / "execution.json")
                and acceptance["lease_release"] == {"status": "RELEASED", "dag_id": c.DAG, "run_id": request["run_id"]}
                and acceptance["official_exported"] is False and acceptance["research_ready"] is False,
                "CATALOG_PROGRESS_ACCEPTANCE_CHANGED")
    c.e.require(type(execution["exit_code"]) is int and execution["exit_code"] == 0 and execution["timed_out"] is False
                and type(execution["wall_seconds"]) in (int, float) and 0 <= execution["wall_seconds"] < 7200,
                "CATALOG_PROGRESS_EXECUTION_NOT_ACCEPTED")
    result = c.session_result(runtime, request, session)
    completed, added = result["completed_months"], result["completed_in_session"]
    c.e.require(type(added) is int and 0 <= added <= completed
                and sorted(p.parent.name for p in (runtime / "months").glob("*/checkpoint.json")) == c.module_months()[:completed],
                "CATALOG_PROGRESS_NOT_LATEST_COMPLETE_PREFIX")
    c.e.require(result["reproduction_months"] == min(completed, 225)
                and result["extension_months"] == max(0, completed - 225), "CATALOG_PROGRESS_SPLIT_CHANGED")
    counts = {}
    if completed:
        last = runtime / "months" / c.module_months()[completed - 1]
        record = c.load(last / "record.json")
        for table in sorted(c.e.TABLES):
            count = record["commit"]["tables"][table]["count"]
            c.e.require(type(count) is int and count >= 0, "CATALOG_PROGRESS_TABLE_COUNT_INVALID")
            counts[table] = count
    return {"version": "dat03-catalog-progress-v1", "status": "ACCEPTED_CATALOG_CHECKPOINT",
            "catalog_request_sha256": c.e.sha(catalog / "request.json"), "session_id": session_id,
            "acceptance_sha256": c.e.sha(session / "acceptance.json"),
            "result_sha256": acceptance["result_sha256"], "execution_sha256": acceptance["execution_sha256"],
            "completed_months": completed, "required_months": 288, "completed_in_session": added,
            "reproduction_months": result["reproduction_months"], "extension_months": result["extension_months"],
            "last_published_month": c.module_months()[completed - 1] if completed else None,
            "catalog_percent": round(completed * 100 / 288, 3), "reached_milestones": milestones(completed),
            "wall_seconds": execution["wall_seconds"], "child_peak_rss_kib": execution["child_peak_rss_kib"],
            "current_snapshots": result["current_snapshots"], "table_counts": counts,
            "lease_release": acceptance["lease_release"],
            "scope": "CATALOG_ONLY_NOT_DAT03_COMPLETION", "research_ready": False, "official_exported": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--runtime", type=Path, required=True, help="Exact retained host observation, not a new Docker copy")
    parser.add_argument("--session", required=True)
    parser.add_argument("--output", type=Path, help="New metadata JSON only; an existing file is never overwritten")
    args = parser.parse_args()
    value = report(args.catalog, args.runtime, args.session)
    if args.output:
        target = c.e.safe(args.output)
        c.e.require(target.suffix == ".json" and target.parent.is_dir()
                    and not target.is_relative_to(c.e.safe(args.catalog))
                    and not target.is_relative_to(c.e.safe(args.runtime)), "CATALOG_PROGRESS_OUTPUT_OUTSIDE_EVIDENCE_REQUIRED")
        c.e.save(target, value)
    else:
        print(json.dumps(value, sort_keys=True, indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
