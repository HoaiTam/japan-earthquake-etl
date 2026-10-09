"""ORC-05 controlled read-only failure/recovery; no HTTP source or lake writes."""
import json
import os
from pathlib import Path
import time
import uuid

import backfill_observability as telemetry
import backfill_runtime as runtime
import run_observability as obs
from jma_backfill_runtime import _atomic_json
from source_run_guard import lease, SourceRunBusy


def run(environment=None, conf=None):
    base = os.environ if environment is None else environment
    env = {**base, "RUN_SUMMARY_ROOT": str(Path(base.get(
        "RUN_SUMMARY_ROOT", "/opt/pipeline/staging/run-summary")) / "qa")}
    conf = dict(conf or json.loads((Path(__file__).parent / "fixtures/orc_03_reuse_sample.json").read_text()))
    conf["preview"] = False
    run_id = "orc05-recovery-qa-" + uuid.uuid4().hex
    plan = telemetry.resolve_observed({"run_id": run_id, "dag_run": {"conf": conf}}, env)
    runtime.require(plan["action"] == "reuse", "QA_READ_ONLY_REQUIRED")
    context = telemetry.context(plan, run_id)
    telemetry.source_lease(plan, run_id, "acquire", env)
    started = time.monotonic()
    try:
        # Persist deliberate failure BEFORE any store invocation; do not corrupt input.
        def fail():
            raise runtime.BackfillError("SOURCE_NOT_READY")
        try:
            obs.observe(context, "source", fail, lambda result: {}, env, attempt=1)
        except obs.ObservabilityError:
            pass
        runtime.require(obs.read_summary(context, env)["status"] == "FAILED", "QA_FAILURE_NOT_SAVED")
        try:
            lease("acquire", {"dag_id": "orc_02_daily_sources", "run_id": run_id + "-contender"}, env)
        except SourceRunBusy:
            contention_blocked = True
        else:
            raise runtime.BackfillError("QA_CONTENTION_NOT_BLOCKED")
        first = runtime.execute(plan, env, attempt=2, telemetry_context=context)
        second = runtime.execute(plan, env, attempt=3, telemetry_context=context)
        runtime.require(first == second, "QA_RERUN_CHANGED")
    finally:
        released = telemetry.source_lease(plan, run_id, "release", env)
    telemetry.complete(plan, run_id, second, released, env)
    summary = obs.read_summary(context, env)
    result = {"task": "ORC-05", "profile_version": "orc-05-local-v1",
              "method": "controlled_failure_then_fresh_readback_not_scheduler", "run_id": run_id,
              "operation_id": plan["operation_id"], "scope_sha256": plan["scope_sha256"],
              "failed_phase": "source", "recovered_phase": "source", "failure_before_store_call": True,
              "status": summary["status"], "published": summary["published"],
              "contention_blocked": contention_blocked, "rerun_unchanged": True,
              "lease_released": released["status"] == "RELEASED", "lake_writes": False,
              "sources_downloaded": False, "duration_seconds": time.monotonic() - started,
              "sources": [{"source_system": item["source_system"], "input": item["counts"]["input"]}
                          for item in summary["sources"]],
              "summary_path": str(obs._root(context, env) / "run_summary.json")}
    _atomic_json(obs._root(context, env) / "recovery_report.json", result)
    return result


if __name__ == "__main__":
    try:
        print(json.dumps(run(), sort_keys=True))
    except Exception:
        raise SystemExit("ORC05_RECOVERY_QA_FAILED") from None
