#!/usr/bin/env python3
"""DAT-03 rollout planning and bounded read-only capacity gate. Never ingest or approve a release."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dat03_profile", ROOT / "scripts/profile-research-archives.py")
profile = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(profile)
VERSION = "dat03-rollout-plan-v1"
SPLITS = {"REPRODUCTION": ("2000-01", "2018-10", 225), "EXTENSION": ("2018-10", "2024-01", 63)}
SERVICES = ("airflow-scheduler", "minio", "iceberg-rest", "trino")


def require(value, reason):
    if not value:
        raise ValueError(reason)


def safe(path):
    path = Path(path).absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)), "ROLLOUT_SYMLINK_REJECTED")
    return path.resolve()


def load(path, limit=4 * 1024**2):
    path = safe(path)
    require(path.is_file() and 0 < path.stat().st_size <= limit, "ROLLOUT_METADATA_BOUND")
    with path.open("rb") as stream:
        raw = stream.read(limit + 1)
    require(len(raw) <= limit, "ROLLOUT_METADATA_BOUND")
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "ROLLOUT_DUPLICATE_JSON_KEY"); result[key] = value
        return result
    def nonfinite(_):
        raise ValueError("ROLLOUT_NONFINITE_JSON")
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=nonfinite)


def save(path, value):
    with safe(path).open("x", encoding="utf-8") as out:
        json.dump(value, out, indent=2, sort_keys=True, allow_nan=False); out.write("\n")


def months(start, end):
    require(re.fullmatch(r"20[0-9]{2}-(0[1-9]|1[0-2])", start) and re.fullmatch(r"20[0-9]{2}-(0[1-9]|1[0-2])", end), "ROLLOUT_MONTH_INVALID")
    result = []; year, month = map(int, start.split("-"))
    while f"{year:04d}-{month:02d}" < end:
        result.append(f"{year:04d}-{month:02d}"); year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    require(result and len(result) <= 480, "ROLLOUT_MONTH_RANGE_INVALID")
    return result


def dependencies(month):
    require(re.fullmatch(r"20[0-9]{2}-(0[1-9]|1[0-2])", month), "ROLLOUT_MONTH_INVALID")
    year, number = map(int, month.split("-"))
    require(2000 <= year <= 2023, "ROLLOUT_MONTH_OUTSIDE_RESEARCH")
    # Every December UTC needs the following JST archive, not only December2023.
    return [year, year + 1] if number == 12 else [year]


def plan(captures):
    captures = safe(captures)
    evidence = load(ROOT / "docs/evidence/DAT-03_RESEARCH_ARCHIVE_PROFILES.json")
    rows = profile.inventory(); sources = []; counts = {}
    require([int(r["year"]) for r in rows] == list(range(2000, 2025)), "ROLLOUT_INVENTORY_YEAR_SET")
    require([r["year"] for r in evidence["archives"]] == list(range(2000, 2025)), "ROLLOUT_PROFILE_YEAR_SET")
    for row, known in zip(rows, evidence["archives"]):
        year = int(row["year"]); folder = safe(captures / f"h{year}")
        load(folder / "capture.json", 65536)  # Strict JSON/size check before the shared capture verifier.
        capture = profile.verified_capture(folder, row)
        report_path = safe(folder / ("profile-" + evidence["runner_sha256"] + ".json")); report = load(report_path)
        require(profile.sha(report_path) == known["profile_sha256"] and report == known["profile"]
                and capture["sha256"] == known["archive_sha256"] and capture["catalog_release"] == known["catalog_release"]
                and all(capture[k] == known[k] for k in ("retrieved_at_utc", "last_modified_utc", "etag", "http_status"))
                and type(capture["http_status"]) is int and type(capture["bytes"]) is int
                and capture["inventory_metadata_drift"] is False and report["parser_version"] == "slv-03-v1.2"
                and report["structural_validation"] == "PASSED" and report["counts"]["parser_rejected"] == 0,
                "ROLLOUT_SOURCE_OR_PROFILE_PIN_CHANGED")
        sources.append({"year": year, "archive_sha256": capture["sha256"], "capture_sha256": profile.sha(folder / "capture.json"),
                        "profile_sha256": known["profile_sha256"], "bytes": capture["bytes"], "catalog_release": capture["catalog_release"]})
        for month, count in report["accepted_rows_by_utc_month"].items():
            require(type(count) is int and count >= 0, "ROLLOUT_PROFILE_COUNT_INVALID")
            counts[month] = counts.get(month, 0) + count
    batches = []; splits = {}
    for split, (start, end, expected) in SPLITS.items():
        required = months(start, end); require(len(required) == expected, "ROLLOUT_SPLIT_COUNT_CHANGED")
        for month in required:
            require(month in counts, "ROLLOUT_PROFILE_MONTH_MISSING")
            # December2022's right halo reaches the CON-01 seed even though its
            # target events predate2023. Never omit that source requirement.
            usgs_months = ["2023-01"] if month == "2022-12" else [month] if month.startswith("2023-") else []
            batches.append({"month": month, "split": split, "jma_years": dependencies(month), "usgs_required": bool(usgs_months),
                            "usgs_capture_months": usgs_months,
                            "profile_accepted_rows": counts[month], "profile_is_coverage_proof": False})
        splits[split] = {"months": expected, "start_utc": start + "-01T00:00:00Z", "end_utc_exclusive": end + "-01T00:00:00Z",
                         "profile_accepted_rows": sum(counts[m] for m in required)}
    require(len(batches) == 288 and len({b["month"] for b in batches}) == 288, "ROLLOUT_BATCH_SET_CHANGED")
    return {"version": VERSION, "status": "PLAN_ONLY", "sources": sources, "splits": splits, "batches": batches,
            "compressed_bytes": sum(s["bytes"] for s in sources), "selected_revision_cap_unchanged": 100000, "active_pin_cap_unchanged": 64,
            "max_affected_months_unchanged": 24, "concurrent_etl_runs": 1, "official_exported": False, "research_ready": False,
            "coverage_verified": False, "full_capacity_accepted": False,
            "required_usgs_months": sorted({m for b in batches for m in b["usgs_capture_months"]}),
            "pending_gates": ["TRINO_FULL_SCALE_CAPACITY", "USGS_FULL_2023_PINS", "288_REAL_MONTH_PUBLICATIONS", "OFFICIAL_COLLECTION_EXPORT"]}


def capacity_sql(report, copies, bucket=None, table="event_current"):
    require(type(copies) is int and 1 <= copies <= 64, "ROLLOUT_TYPED_CAPACITY_COPIES")
    require(bucket is None or type(bucket) is str and re.fullmatch(r"[0-9a-f]", bucket), "ROLLOUT_EXACT_CAPACITY_BUCKET")
    require(table in ("event_current", "event_source_bridge"), "ROLLOUT_EXACT_CAPACITY_TABLE")
    namespace = report["namespace"]
    require(re.fullmatch(r"iceberg\.dat03_e2e_[a-f0-9]{32}", namespace), "ROLLOUT_EXACT_DIAGNOSTIC_NAMESPACE")
    require([c["month"] for c in report["cases"]] == ["2000-01", "2011-04", "2016-04", "2023-12"], "ROLLOUT_DIAGNOSTIC_CASE_SET")
    commit = report["cases"][3]["first"]["commit"]
    before = commit["scope"]["baseline_snapshots"][table]; after = commit["tables"][table]["snapshot_id"]
    require(all(type(p) is int and 0 < p <= 9223372036854775807 for p in (before, after)), "ROLLOUT_EXACT_DIAGNOSTIC_PINS")
    prior = report["cases"][2]["first"]["commit"]["tables"][table]
    require(prior["snapshot_id"] == before and commit["scope"]["affected_months"] == ["2023-12"]
            and type(prior["count"]) is int and 0 < prior["count"] <= 10000000, "ROLLOUT_DIAGNOSTIC_BASELINE_CHANGED")
    def side(snapshot):
        month = "format_datetime(e.event_time_utc AT TIME ZONE 'UTC','yyyy-MM')" if table == "event_current" else "e.event_month_utc"
        return (f"SELECT e.*, n.copy_id AS dat03_capacity_copy FROM {namespace}.{table} FOR VERSION AS OF {snapshot} AS e "
                f"CROSS JOIN UNNEST(sequence(1,{copies})) AS n(copy_id) "
                f"WHERE {month} <> '2023-12'"
                + ("" if bucket is None else " AND lower(substr(to_hex(sha256(to_utf8(coalesce(e.canonical_event_id,'')))),1,1))='" + bucket + "'"))
    a, b = side(before), side(after)
    return f"SELECT count(*) AS changed_rows FROM (({a} EXCEPT {b}) UNION ALL ({b} EXCEPT {a})) changes", prior["count"] * copies


def services():
    names = ["japan-earthquake-etl-" + s + "-1" for s in SERVICES]
    run = subprocess.run(["docker", "inspect", *names], check=True, capture_output=True, text=True, timeout=20)
    rows = json.loads(run.stdout)
    require(len(rows) == 4 and all(r["State"]["Status"] == "running" and r["State"].get("Health", {}).get("Status") == "healthy" for r in rows), "ROLLOUT_SERVICES_NOT_READY")
    return {r["Name"]: {"id": r["Id"], "started_at": r["State"]["StartedAt"], "restarts": r["RestartCount"]} for r in rows}


def classify_probe(code, stdout, stderr):
    if code == 0:
        try:
            value = json.loads(stdout)
            require(type(value) is dict and set(value) == {"changed_rows"} and type(value["changed_rows"]) is int and value["changed_rows"] == 0, "ROLLOUT_CAPACITY_READBACK_INVALID")
            return "STRESS_PROBE_PASSED"
        except (ValueError, TypeError, KeyError):
            return "READBACK_INVALID"
    text = stderr.lower()
    if "memory limit" in text or "exceeded_local_memory_limit" in text or "exceeded_global_memory_limit" in text:
        return "BLOCKED_MEMORY"
    if "time limit" in text or "exceeded_time_limit" in text:
        return "BLOCKED_TIME"
    return "FAILED_REVIEW_PRIVATE_LOG"


def idle():
    # Read the metadata database directly: importing the Airflow application can
    # exceed the preflight deadline and is unnecessary for a read-only count.
    script = '''import os
from pathlib import Path
from sqlalchemy import create_engine,text
from sqlalchemy.pool import NullPool
guard=Path(os.environ["SOURCE_GUARD_ROOT"])
assert not (guard/"owner.json").exists(), "ROLLOUT_LEASE_NOT_EMPTY"
engine=create_engine(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"],poolclass=NullPool,
    connect_args={"connect_timeout":10,"options":"-c default_transaction_read_only=on -c statement_timeout=10000"})
try:
    with engine.connect() as connection:
        assert connection.execute(text("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')")).scalar_one()==0, "ROLLOUT_ACTIVE_DAGRUN"
finally:
    engine.dispose()
for proc in Path("/proc").iterdir():
    if proc.name.isdigit():
        try: command=(proc/"comm").read_text().strip()
        except FileNotFoundError: continue
        assert command != "java", "ROLLOUT_ACTIVE_JAVA"
print("ROLLOUT_IDLE")
'''
    checked = subprocess.run(["docker", "exec", "japan-earthquake-etl-airflow-scheduler-1", "python3", "-c", script],
                             check=False, capture_output=True, text=True, timeout=30)
    require(checked.returncode == 0 and checked.stdout.strip() == "ROLLOUT_IDLE", "ROLLOUT_IDLE_PREFLIGHT_FAILED")


def trino_queries_idle():
    script = '''import os,json,urllib.request
url="http://"+os.environ["TRINO_HOST"]+":"+os.environ["TRINO_INTERNAL_PORT"]+"/v1/query"
with urllib.request.urlopen(urllib.request.Request(url,headers={"X-Trino-User":"dat03-capacity-probe"}),timeout=15) as response:
    raw=response.read(4*1024*1024+1)
assert len(raw)<=4*1024*1024
rows=json.loads(raw)
assert isinstance(rows,list) and all(isinstance(row,dict) and row.get("state") in ("FINISHED","FAILED") for row in rows)
print("ROLLOUT_TRINO_IDLE")
'''
    checked = subprocess.run(["docker", "exec", "japan-earthquake-etl-airflow-scheduler-1", "python3", "-c", script],
                             check=False, capture_output=True, text=True, timeout=25)
    require(checked.returncode == 0 and checked.stdout.strip() == "ROLLOUT_TRINO_IDLE", "ROLLOUT_TRINO_QUERY_NOT_IDLE")


def probe(deployment, runtime, copies, strategy="full", query_seconds=90, table="event_current", total_query_seconds=600):
    require(strategy in ("full", "buckets-v1"), "ROLLOUT_CAPACITY_STRATEGY_INVALID")
    require(type(query_seconds) is int and 30 <= query_seconds <= 300, "ROLLOUT_QUERY_BUDGET_INVALID")
    require(table in ("event_current", "event_source_bridge"), "ROLLOUT_EXACT_CAPACITY_TABLE")
    require(type(total_query_seconds) is int and 600 <= total_query_seconds <= 3600, "ROLLOUT_TOTAL_QUERY_BUDGET_INVALID")
    deployment, runtime = safe(deployment), safe(runtime)
    expected = load(ROOT / "docs/evidence/DAT-03_CONTINUATION.json")
    require(runtime == deployment / "staging/dat03-continuation" / expected["checkpoint_recovery"]["attempt_id"] / "runtime", "ROLLOUT_EXACT_DIAGNOSTIC_ROOT")
    require(profile.sha(runtime / "report.json") == expected["checkpoint_recovery"]["report_sha256"]
            and profile.sha(runtime / "acceptance-recovery.json") == expected["checkpoint_recovery"]["acceptance_sha256"], "ROLLOUT_DIAGNOSTIC_EVIDENCE_CHANGED")
    report = load(runtime / "report.json")
    queries = [capacity_sql(report, copies, b, table)[0] for b in ("0123456789abcdef" if strategy == "buckets-v1" else [None])]
    _, expanded = capacity_sql(report, copies, table=table)
    before = services(); idle(); trino_queries_idle()
    target = safe(deployment / "staging/dat03-capacity" / uuid.uuid4().hex); target.mkdir(parents=True, mode=0o700)
    # V2 separates the overall sequential measurement window from each query's
    # unchanged engine limit. It never relaxes a failed query or accepts a prefix.
    version = "dat03-trino-stress-v1" if table == "event_current" and total_query_seconds == 600 else "dat03-trino-stress-v2"
    request = {"version": version, "namespace": report["namespace"], "report_sha256": expected["checkpoint_recovery"]["report_sha256"],
                                  "copies": copies, "stress_rows_per_side": expanded, "strategy": strategy,
                                  "queries": queries, "query_max_run_time_seconds": query_seconds, "total_probe_budget_seconds": total_query_seconds}
    if version.endswith("v2"):
        request["table"] = table
    save(target / "request.json", request)
    started = time.monotonic(); results = []; status = "STRESS_PROBE_PASSED"
    for number, query in enumerate(queries):
        remaining = total_query_seconds - (time.monotonic() - started)
        if remaining < query_seconds + 40:
            status = "PROBE_BUDGET_EXHAUSTED_NOT_ACCEPTED"; break
        command = ["docker", "exec", "japan-earthquake-etl-trino-1", "trino", "--user", "dat03-capacity-probe", "--output-format", "JSON",
                   "--session", f"query_max_run_time={query_seconds}s", "--session", f"query_max_execution_time={query_seconds}s", "--execute", query]
        query_started = time.monotonic()
        try:
            result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=query_seconds + 40)
            status = classify_probe(result.returncode, result.stdout, result.stderr)
            stdout, stderr, exit_code = result.stdout, result.stderr, result.returncode
        except subprocess.TimeoutExpired as timeout:
            # Retain partial output; do not resubmit a query with unknown outcome.
            stdout, stderr, exit_code = timeout.stdout or b"", timeout.stderr or b"", None
            status = "UNKNOWN_QUERY_OUTCOME_REQUIRES_READONLY_DIAGNOSIS"
        for name, content in (("stdout", stdout), ("stderr", stderr)):
            if isinstance(content, bytes): content = content.decode("utf-8", errors="replace")
            with (target / f"{number:02d}-{name}.private").open("x", encoding="utf-8") as out:
                os.chmod(target / f"{number:02d}-{name}.private", 0o600); out.write(content)
        if isinstance(stderr, bytes): stderr = stderr.decode("utf-8", errors="replace")
        match = re.search(r"\b[0-9]{8}_[0-9]{6}_[0-9]{5}_[a-z0-9]{5}\b", stderr)
        results.append({"number": number, "query_id": match.group(0) if match else None, "exit_code": exit_code,
                        "status": status, "wall_seconds": round(time.monotonic() - query_started, 3)})
        print(f"event=dat03_capacity_query number={number} status={status}", flush=True)
        if status != "STRESS_PROBE_PASSED": break
    after = None; postflight = "UNCONFIRMED"
    try:
        after = services(); idle(); trino_queries_idle()
        postflight = "PASSED_UNCHANGED" if before == after else "SERVICE_CHANGED"
    except (subprocess.SubprocessError, ValueError, KeyError, OSError):
        pass  # Still emit a failed receipt; never lose the exact query outcome/logs.
    if postflight != "PASSED_UNCHANGED" and status == "STRESS_PROBE_PASSED":
        status = "POSTFLIGHT_FAILED_NOT_ACCEPTED"
    receipt = {"version": version, "status": status, "strategy": strategy, "queries": results,
               "required_query_count": len(queries), "completed_query_count": len(results),
               "wall_seconds": round(time.monotonic() - started, 3), "stress_rows_per_side": expanded, "copies": copies,
               "request_sha256": profile.sha(target / "request.json"), "services_before": before, "services_after": after,
               "postflight_status": postflight,
               "data_mutations": False, "lease_acquired_or_released": False, "full_capacity_accepted": False,
               "synthetic_expansion_is_not_research_data": True, "research_ready": False}
    if version.endswith("v2"):
        receipt["table"] = table
    save(target / "receipt.json", receipt)
    print("event=dat03_capacity_probe status=" + status + " evidence=" + str(target), flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("mode", choices=("plan", "probe"))
    parser.add_argument("--captures", type=Path, default=ROOT / "staging/dat03-research-profile-20261024")
    parser.add_argument("--output", type=Path); parser.add_argument("--deployment-root", type=Path); parser.add_argument("--runtime-root", type=Path)
    parser.add_argument("--copies", type=int, default=32)
    parser.add_argument("--query-seconds", type=int, default=90)
    parser.add_argument("--table", choices=("event_current", "event_source_bridge"), default="event_current")
    parser.add_argument("--total-query-seconds", type=int, default=600)
    parser.add_argument("--strategy", choices=("full", "buckets-v1"), default="full"); args = parser.parse_args()
    if args.mode == "plan":
        result = plan(args.captures)
        if args.output:
            output = safe(args.output); require(output.is_relative_to(ROOT / "staging"), "ROLLOUT_STAGING_OUTPUT_REQUIRED"); save(output, result)
        print(json.dumps({"version": result["version"], "batches": len(result["batches"]), "splits": result["splits"], "status": result["status"], "full_capacity_accepted": False}))
    else:
        require(args.deployment_root is not None and args.runtime_root is not None, "ROLLOUT_PROBE_PATHS_REQUIRED")
        result = probe(args.deployment_root, args.runtime_root, args.copies, args.strategy, args.query_seconds, args.table, args.total_query_seconds)
        if result["status"] != "STRESS_PROBE_PASSED":
            raise SystemExit(2)


if __name__ == "__main__":
    main()
