"""ORC-03 read-only reuse QA by default; --source-pilot opts into bounded Bronze ingest."""
from copy import deepcopy
import argparse
import json
import os
from pathlib import Path
import uuid

from backfill_runtime import execute, preview_summary, require, resolve, verify_pinned
from jma_backfill_runtime import _atomic_json
from source_run_guard import lease


def run(environment=None, source_pilot=False, observability=False):
    env = os.environ if environment is None else environment
    if observability:
        require(not source_pilot, "QA_READ_ONLY_REQUIRED")
        return telemetry_run(env)
    conf = json.loads((Path(__file__).parent / "fixtures/orc_03_reuse_sample.json").read_text())
    qa_id = "orc03-reuse-qa-" + uuid.uuid4().hex
    outside_pins = deepcopy(conf["bronze_inputs"])
    outside_plan = resolve({"dag_run": {"conf": conf}}, env)
    if source_pilot:
        qa_id = "orc03-source-qa-" + uuid.uuid4().hex
        conf = {"operation_id": qa_id, "processing_date": "2026-10-08", "action": "ingest", "preview": True,
                "usgs": {"window_start_utc": "2023-01-01T00:00:00Z", "window_end_utc": "2023-01-03T00:00:00Z", "chunk_days": 1},
                "jma": {"years": [1997], "segments": [{"year": 1997, "segment": "oct-dec"}]}}
    context = {"dag_run": {"conf": conf}}
    preview = resolve(context, env)
    require(preview_summary(preview)["published"] is False, "QA_PREVIEW_FAILED")
    conf = deepcopy(conf); conf["preview"] = False
    plan = resolve({"dag_run": {"conf": conf}}, env)
    identity = {"dag_id": "orc_03_backfill", "run_id": qa_id}
    acquired = lease("acquire", identity, env)
    try:
        if source_pilot:
            verify_pinned(outside_plan, outside_pins, env)
        first = execute(plan, env)
        rerun = execute(plan, env)
        require(first == rerun and first["status"] == "BronzeVerified" and first["published"] is False,
                "QA_RERUN_CHANGED")
        if source_pilot:
            verify_pinned(outside_plan, outside_pins, env)
        report = {"evidence_version": "orc-03-v1", "status": "VERIFIED", "method": "runtime_api_not_scheduler",
                  "run_id": identity["run_id"], "operation_id": plan["operation_id"],
                  "scope_sha256": plan["scope_sha256"], "preview_verified": False,
                  "reuse_readback_verified": True, "same_operation_rerun_verified": True,
                  "published": False, "sources_downloaded": source_pilot, "lake_writes": source_pilot,
                  "source_pilot": source_pilot, "outside_pinned_inputs_unchanged": True if source_pilot else None,
                  "bronze_inputs": first["bronze_inputs"], "lease_acquired": acquired["status"]}
    finally:
        released = lease("release", identity, env)
    require(released["status"] == "RELEASED", "QA_LEASE_RELEASE_FAILED")
    report["lease_released"] = True
    report["normal_runner_replaced"] = False
    target = Path(env.get("BACKFILL_STAGING_ROOT", "/opt/pipeline/staging/backfill")) / "qa" / identity["run_id"] / "report.json"
    _atomic_json(target, report)
    return {**report, "report_path": str(target)}


def telemetry_run(environment):
    import backfill_observability as telemetry
    import run_observability as obs
    env = {**environment, "RUN_SUMMARY_ROOT": str(Path(environment.get(
        "RUN_SUMMARY_ROOT", "/opt/pipeline/staging/run-summary")) / "qa")}
    conf = json.loads((Path(__file__).parent / "fixtures/orc_03_reuse_sample.json").read_text())
    conf["preview"] = False
    run_id = "orc04-backfill-qa-" + uuid.uuid4().hex
    airflow = {"run_id": run_id, "dag_run": {"conf": conf}}
    summaries = []
    for attempt in (1, 2):
        plan = telemetry.resolve_observed(airflow, env, attempt)
        telemetry.source_lease(plan, run_id, "acquire", env, attempt)
        resolved = telemetry.context(plan, run_id)
        try:
            result = execute(plan, env, attempt, resolved)
        finally:
            released = telemetry.source_lease(plan, run_id, "release", env, attempt)
        telemetry.complete(plan, run_id, result, released, env, attempt)
        summaries.append(obs.read_summary(resolved, env))
    require(summaries[0]["sources"] == summaries[1]["sources"]
            and summaries[1]["status"] == "BronzeVerified" and summaries[1]["published"] is False,
            "QA_RERUN_CHANGED")
    return {"task": "ORC-04", "method": "runtime_api_fresh_exact_bronze_readback_not_scheduler",
            "dag_id": resolved["dag_id"], "run_id": run_id, "operation_id": plan["operation_id"],
            "scope_sha256": plan["scope_sha256"], "status": summaries[1]["status"], "published": False,
            "source_counts": [{"source_system": row["source_system"], "input": row["counts"]["input"],
                               "parsed": row["counts"]["parsed"], "fetched": row["counts"]["fetched"]}
                              for row in summaries[1]["sources"]],
            "same_run_rerun_counts_unchanged": True, "lease_released": released["status"] == "RELEASED",
            "sources_downloaded": False, "lake_writes": False,
            "summary_path": str(obs._root(resolved, env) / "run_summary.json")}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-pilot", action="store_true",
                        help="Explicit opt-in: ingest two USGS day chunks + one JMA segment, then rerun")
    parser.add_argument("--observability", action="store_true", help="Read-only ORC-04 observed reuse/rerun QA")
    args = parser.parse_args()
    print(json.dumps(run(source_pilot=args.source_pilot, observability=args.observability), sort_keys=True))
