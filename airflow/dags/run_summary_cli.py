"""Inspect ORC-04 metadata or run bounded offline/runtime telemetry smoke.

No source GET, MinIO data write, scheduler trigger or DAG pause-state change.
"""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import sys
import uuid

import etl_pipeline_runtime as etl
import run_observability as obs


def smoke(root):
    env = {"RUN_SUMMARY_ROOT": root, "CONFIG_VERSION": "orc-04-smoke-v1"}
    run_id = "orc04-smoke-" + uuid.uuid4().hex
    ctx = {"run_id": run_id, "dag_run": {"conf": {"mode": "mock"}}}
    context = etl.resolve_observed_context(ctx, env)
    def chain():
        previous = None
        for phase in etl.PHASES:
            previous = etl.execute_observed_phase(phase, context, previous, env)
        etl.observed_publication_summary(context, previous, env)
        return obs.read_summary(context, env)
    first = chain()
    etl.resolve_observed_context(ctx, env, attempt=2)
    # Deliberate safe failure at the real DAG runtime wrapper boundary, not a dataset write.
    try:
        obs.observe(context, "readiness", lambda: (_ for _ in ()).throw(
            etl.EtlContractError("SOURCE_NOT_READY")), lambda result: {}, env, attempt=2)
    except obs.ObservabilityError:
        pass
    failed = obs.read_summary(context, env)
    obs.require(failed["status"] == "FAILED" and not failed["published"] and failed["snapshots"] == [],
                "SMOKE_FAILED")
    etl.resolve_observed_context(ctx, env, attempt=3)
    rerun = chain()
    obs.require(first["sources"] == rerun["sources"] and first["gold_counts"] == rerun["gold_counts"]
                and rerun["status"] == "MockComplete" and not rerun["published"], "SMOKE_FAILED")
    return {"task": "ORC-04", "method": "runtime_api_not_scheduler", "run_id": run_id,
            "summary_path": str(obs._root(obs.identity(context), env) / "run_summary.json"),
            "checks": {"mock_success": True, "failed_run_persisted": True,
                       "stale_publication_invalidated": True, "rerun_counts_unchanged": True},
            "counts": {"USGS_input": 2, "JMA_input": 2, "canonical_current": 4},
            "published": False, "evidence_kind": "synthetic"}


def inspect_summary(dag_id, run_id, root):
    obs.require(dag_id in {"orc_01_etl_pipeline", "orc_02_daily_sources"}, "UNKNOWN_DAG")
    seed = {"dag_id": dag_id, "run_id": obs.label(run_id)}
    env = {"RUN_SUMMARY_ROOT": root}
    folder = obs._root(seed, env)
    # Inspect is read-only, even when the requested run does not exist.
    obs.require(folder.is_dir(), "SUMMARY_NOT_FOUND")
    with obs._locked(folder):
        state = folder / "state.json"
        obs.require(state.stat().st_size <= obs.MAX_BYTES, "TELEMETRY_TOO_LARGE")
        context = json.loads(state.read_text())["run"]
    value = obs.read_summary(context, env)
    # Revalidate projected files before printing (no generic stdout of an untrusted file).
    with obs._locked(folder):
        saved = json.loads(state.read_text())
        for phase, event in saved["phases"].items():
            obs.validate_details(event["details"], phase, saved["run"])
        obs.identity(saved["run"])
        expected = obs._summary(saved)
        obs.require(expected == value, "STALE_RUN_SUMMARY")
    return value


def source_evidence(plan_path, summary_path, root):
    """Replay exact saved ORC-02 receipts, not a claim of a fresh Bronze readback.

Only read two explicitly named <=256KiB JSON metadata files. Never list buckets
or parse business bytes. A new QA run ID prevents impersonating the original run.
"""
    def read(path):
        target = Path(path)
        obs.require(target.is_file() and target.stat().st_size <= obs.MAX_BYTES, "INVALID_SOURCE_EVIDENCE")
        return json.loads(target.read_text())
    plan, summary = read(plan_path), read(summary_path)
    obs.require(summary.get("status") == "SourcesReady" and summary.get("verified") is True
                and summary.get("published") is False and summary.get("run_id") == plan.get("run_id")
                and summary.get("usgs_context") == plan.get("usgs")
                and summary.get("jma_scope") == plan["jma"]["years"],
                "INVALID_SOURCE_EVIDENCE")
    jma = obs.source_details("JMA_BULLETIN", summary["jma"]["archives"])
    usgs = obs.source_details("USGS", [summary["usgs"]], plan["usgs"])
    expected = {(item["year"], item["segment"]) for item in plan["jma"]["archives"]}
    obs.require({(item["year"], item["segment"]) for item in jma["bronze_inputs"]} == expected
                and len(jma["bronze_inputs"]) == len(expected), "INVALID_SOURCE_EVIDENCE")
    pins = [{"source_system": item["source_system"], "manifest_uri": item["manifest_uri"],
             "sha256": item["manifest_sha256"]} for item in [*usgs["bronze_inputs"], *jma["bronze_inputs"]]]
    obs.require(summary.get("input_manifests") == pins, "INVALID_SOURCE_EVIDENCE")
    original_run_id = plan["run_id"]
    qa_plan = deepcopy(plan)
    qa_plan["run_id"] = "orc04-source-evidence-" + uuid.uuid4().hex
    context = obs.source_context(qa_plan)
    context["mode"] = "replay"
    env = {"RUN_SUMMARY_ROOT": root}
    obs.observe(context, "readiness", lambda: jma, lambda result: result, env)
    obs.observe(context, "bronze", lambda: usgs, lambda result: result, env)
    obs.observe(context, "complete", lambda: None,
                lambda result: {"completion_status": "MetadataReplayComplete"}, env)
    # No SourcesReady claim: this is a historical metadata serialization check.
    result = obs.read_summary(context, env)
    return {"task": "ORC-04", "method": "exact_saved_source_metadata_not_fresh_readback",
            "original_run_id": obs.label(original_run_id), "qa_run_id": context["run_id"],
            "source_counts": [{"source_system": row["source_system"], "input": row["counts"]["input"],
                               "parsed": row["counts"]["parsed"], "manifests": len(row["bronze_inputs"])}
                              for row in result["sources"]],
            "published": False, "summary_path": str(obs._root(obs.identity(context), env) / "run_summary.json")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--smoke", action="store_true")
    mode.add_argument("--inspect", action="store_true")
    mode.add_argument("--source-evidence", action="store_true")
    parser.add_argument("--root", default=os.environ.get("RUN_SUMMARY_ROOT", "staging/run-summary"))
    parser.add_argument("--dag-id", default="orc_01_etl_pipeline")
    parser.add_argument("--run-id")
    parser.add_argument("--plan-file")
    parser.add_argument("--source-summary-file")
    args = parser.parse_args()
    try:
        if args.smoke:
            result = smoke(args.root)
        elif args.source_evidence:
            obs.require(args.plan_file and args.source_summary_file, "EXACT_SOURCE_FILES_REQUIRED")
            result = source_evidence(args.plan_file, args.source_summary_file, args.root)
        else:
            result = inspect_summary(args.dag_id, args.run_id, args.root)
        print(json.dumps(result, indent=2, allow_nan=False))
    except Exception as error:
        # Never dump filesystem/JSON/environment details or raw exception on an operator CLI.
        reason = str(error) if isinstance(error, obs.ObservabilityError) else "OBSERVABILITY_CLI_FAILED"
        print(reason, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
