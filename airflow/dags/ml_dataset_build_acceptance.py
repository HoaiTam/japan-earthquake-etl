"""Read-only exact-DagRun collector. No trigger/clear/SQL writes or lease release."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.request

from ml_dataset_real import DAG, STEPS, checked, read, require, roots, summary


def query(statement, env):
    require(statement.startswith("SELECT "), "MLI02_READ_ONLY_SQL_REQUIRED")
    host, port = env["TRINO_HOST"], env["TRINO_INTERNAL_PORT"]
    require(all(c in "abcdefghijklmnopqrstuvwxyz0123456789.-" for c in host)
            and port.isdigit(), "MLI02_TRINO_CONFIG_REQUIRED")
    request = urllib.request.Request("http://" + host + ":" + port + "/v1/statement", statement.encode(),
                                     headers={"X-Trino-User": "mli02-readback"}, method="POST")
    rows, deadline = [], time.monotonic() + 120
    while True:
        with urllib.request.urlopen(request, timeout=30) as response:
            page = json.load(response)
        require("error" not in page, "MLI02_READBACK_SQL_FAILED")
        rows.extend(page.get("data", []))
        require(len(rows) <= 64 and time.monotonic() < deadline, "MLI02_READBACK_BOUNDED_REQUIRED")
        if "nextUri" not in page: return rows
        # Only follow this configured Trino endpoint, never caller-provided URLs.
        require(page["nextUri"].startswith("http://" + host + ":" + port + "/"), "MLI02_READBACK_ENDPOINT_CHANGED")
        request = urllib.request.Request(page["nextUri"], headers={"X-Trino-User": "mli02-readback"})


def collect(run, env, compare=None):
    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import create_session
    with create_session() as session:
        row = session.query(DagRun).filter(DagRun.dag_id == DAG, DagRun.run_id == run).one()
        require(row.state == "success", "MLI02_SUCCESSFUL_DAGRUN_REQUIRED")
        instances = session.query(TaskInstance).filter(TaskInstance.dag_id == DAG, TaskInstance.run_id == run).all()
        tasks = {r.task_id: str(r.state) for r in instances}
        durations = {r.task_id: r.duration for r in instances}
        tries = {r.task_id: r.try_number for r in instances}
        timing = {"start_utc": row.start_date.isoformat(), "end_utc": row.end_date.isoformat(),
                  "seconds": (row.end_date - row.start_date).total_seconds()}
        expected = {"resolve_run_context", "mark_dataset_exported.contract_gate"} | {step + ".execute_step" for step in STEPS}
        require(set(tasks) == expected and set(tasks.values()) == {"success"}, "MLI02_TWELVE_TASKS_REQUIRED")
        require(session.query(DagRun).filter(DagRun.state.in_(["running", "queued"])).count() == 0, "MLI02_ACTIVE_RUN_PRESENT")
        require(session.query(DagModel).filter(DagModel.dag_id == "orc_02_daily_sources").one().is_paused, "MLI02_DAILY_MUST_REMAIN_PAUSED")
    slug = "mli02-" + hashlib.sha256((DAG + "|" + run).encode()).hexdigest()[:40]
    root = Path(roots(env)["run_base"]) / slug
    context = read(root / "context.json")
    checked(context, env)
    result = summary(context, read(root / "receipts/mark_dataset_exported.json"), env, persist=False)
    export_phase = read(root / "receipts/export_parquet.json")
    require(not Path(env["SOURCE_GUARD_ROOT"], "owner.json").exists(), "MLI02_LEASE_PRESENT")
    ns, id = context["ml_namespace"], context["dataset_id"]
    require(id.startswith("ds_") and all(c in "0123456789abcdef" for c in id[3:]), "MLI02_EXACT_DATASET_REQUIRED")
    out, sql = result["export"], []
    def scalar(statement, expected):
        rows = query(statement, env)
        require(rows == [[expected]], "MLI02_READBACK_MISMATCH")
        sql.append({"sql": statement, "value": str(expected)})
    pins = (("dataset_manifest", "registry_snapshot_id", 1), ("mainshock_candidate_snapshot", "mainshock_snapshot_id", out["mainshock_count"]),
            ("sequence_candidate_snapshot", "candidate_snapshot_id", out["feature_row_count"]))
    for table, pin, count in pins:
        value = out[pin]
        require(isinstance(value, str) and value.isdigit(), "MLI02_DECIMAL_PIN_REQUIRED")
        scalar("SELECT count(*) FROM " + ns + "." + table + " FOR VERSION AS OF " + value + " WHERE dataset_id='" + id + "'"
               + (" AND dataset_status='EXPORTED'" if table == "dataset_manifest" else ""), count)
        if table != "dataset_manifest":
            scalar("SELECT count(*) FROM " + ns + '."' + table + "$snapshots\" WHERE element_at(summary,'ml.dataset_id')='" + id + "'", 1)
    physical = context["plan"]["gold_publication"]["physical_gold_table_name"]
    gold = physical.rsplit(".", 1)[0]
    pub = context["plan"]["gold_publication"]["gold_publication_id"]
    rows = query("SELECT snapshot_bundle_json FROM " + gold + ".publication_status WHERE publication_id='" + pub + "' AND verify_status='PASSED'", env)
    require(len(rows) == 1 and len(rows[0]) == 1, "MLI02_PUBLICATION_READBACK_REQUIRED")
    bundle = json.loads(rows[0][0]); require(set(bundle) == {"event_current", "event_source_bridge", "dim_date",
        "dim_region", "dim_magnitude_band", "dim_depth_band"}, "MLI02_SIX_GOLD_PINS_REQUIRED")
    gold_pins = {}
    for name, ref in sorted(bundle.items()):
        require(name.replace("_", "").isalnum() and ref["table"] == gold + "." + name, "MLI02_GOLD_TABLE_REQUIRED")
        scalar("SELECT snapshot_id FROM " + gold + '."' + name + '$snapshots" ORDER BY committed_at DESC LIMIT 1', ref["snapshot_id"])
        gold_pins[name] = str(ref["snapshot_id"])
    if compare:
        other = "mli02-" + hashlib.sha256((DAG + "|" + compare).encode()).hexdigest()[:40]
        first = read(Path(roots(env)["run_base"]) / other / "run-summary.json")
        require(first["dataset_id"] == id, "MLI02_RERUN_IDENTITY_CHANGED")
        for key in ("registry_snapshot_id", "mainshock_snapshot_id", "candidate_snapshot_id", "dataset_manifest_sha256", "bundle_manifest_sha256", "checksums_sha256"):
            require(first["export"][key] == out[key], "MLI02_RERUN_PIN_CHANGED")
        require(out["reused"] is True, "MLI02_RERUN_NOT_REUSED")
    return {"task": "MLI-02", "status": "PASSED", "run_id": run, "tasks": tasks, "summary": result,
            "task_durations_seconds": durations, "task_try_numbers": tries, "run_timing": timing,
            "export_phase_reused": export_phase.get("reused"),
            "run_root": str(root), "source_gold_pins_unchanged": gold_pins, "sql_readbacks": sql,
            "sql_query_count": len(sql) + 1, "compared_run": compare, "lease_absent": True, "read_only": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--compare-run")
    args = parser.parse_args()
    print(json.dumps(collect(args.run_id, dict(os.environ), args.compare_run), sort_keys=True))
