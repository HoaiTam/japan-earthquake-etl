#!/usr/bin/env python3
"""DAT-03 exact, checkpointed research rollout. No build/start/restart, source download, DAG or cloud mutations."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SPEC = importlib.util.spec_from_file_location("catalog_support", HERE / "research-e2e.py")
e = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(e)
VERSION = "dat03-catalog-rollout-v1"
DAG = "dat03_catalog_rollout"
CHECK = "ie212.earthquake.spark.usgs.ResearchCatalogJob"
CONTINUATION_VERSION = "dat03-catalog-continuation-v1"
TABLES = ("event_current", "event_source_bridge")
NAMES = tuple([f"h{y}" for y in range(2000, 2025)] + [f"usgs-2023-{m:02d}" for m in range(1, 13)])


def module(name):
    spec = importlib.util.spec_from_file_location("catalog_" + name.replace("-", "_"), HERE / (name + ".py"))
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result); return result


def compatibility(original, current):
    base = "ie212/earthquake/spark/"
    changed_allowed = {base+n+".class" for n in ("usgs/ResearchCatalogJob", "usgs/ResearchCatalogState", "orchestration/FullMonthBronzeReader",
                                                "orchestration/FullMonthStateResolver", "orchestration/RealEtlAdapter", "gold/GoldPublisher", "gold/GoldVerificationPlan")}
    added_allowed = {base+n+".class" for n in ("usgs/ResearchCatalogBatchPlan", "usgs/ResearchCatalogCompatibility", "orchestration/FullMonthBronzeReader$JmaStructureCache")}
    proof = {"version": CONTINUATION_VERSION, "changed_entries": {}, "added_entries": {}, "matched_original_entries": 0}; total = 0
    with zipfile.ZipFile(e.safe(original)) as a, zipfile.ZipFile(e.safe(current)) as b:
        e.require(len(a.namelist()) == len(set(a.namelist())) and len(b.namelist()) == len(set(b.namelist())), "CATALOG_DUPLICATE_JAR_ENTRY")
        for item in a.infolist():
            name = item.filename
            if item.is_dir() or name == "META-INF/MANIFEST.MF": continue
            e.require(name in b.namelist(), "CATALOG_ORIGINAL_CODE_ENTRY_MISSING")
            e.require(item.file_size <= 16*1024**2 and b.getinfo(name).file_size <= 16*1024**2, "CATALOG_JAR_EXPANSION_BOUND")
            before, after = a.read(name), b.read(name); total += len(before) + len(after)
            e.require(total <= 256*1024**2, "CATALOG_JAR_EXPANSION_BOUND")
            if before == after: proof["matched_original_entries"] += 1
            else:
                e.require(name in changed_allowed, "CATALOG_UNREVIEWED_DATA_PLANE_CHANGE")
                proof["changed_entries"][name] = {"original_sha256": e.hashlib.sha256(before).hexdigest(), "continuation_sha256": e.hashlib.sha256(after).hexdigest()}
        for item in b.infolist():
            if item.filename not in a.namelist() and not item.is_dir():
                e.require(item.filename in added_allowed and item.file_size <= 16*1024**2, "CATALOG_UNREVIEWED_ADDED_CODE")
                proof["added_entries"][item.filename] = e.hashlib.sha256(b.read(item.filename)).hexdigest()
    return proof


def load(path, limit=4*1024**2):
    path = e.safe(path)
    e.require(path.is_file() and 0 < path.stat().st_size <= limit, "CATALOG_METADATA_BOUND")
    def pairs(items):
        result = {}
        for key, value in items:
            e.require(key not in result, "CATALOG_DUPLICATE_JSON_KEY"); result[key] = value
        return result
    def bad(_): raise ValueError("CATALOG_NONFINITE_JSON")
    return json.loads(path.read_bytes(), object_pairs_hook=pairs, parse_constant=bad)


def source_files(name):
    e.require(name in NAMES, "CATALOG_EXACT_SOURCE_NAME")
    return ("inputs/" + name + ".json", "inputs/" + name + (".zip" if name.startswith("h") else ".geojson"))


def files():
    result = {"runner.jar", "controller.py", "research-e2e.py", "request.json", "plan.json", "usgs-summary.json",
              "baseline.csv", "boundary.csv", "bounded-report.json"}
    for name in NAMES: result.update(source_files(name))
    for table in TABLES:
        result.update({f"capacity/{table}-{part}.json" for part in ("request", "receipt")})
        result.update({f"capacity/{table}-{i:02d}-stdout.private" for i in range(16)})
    return result


def expected_queries(report, copies, table):
    # Kept equivalent to research-rollout.capacity_sql by a cross-generator test.
    # This only creates read-only verification SQL; all event ETL remains Java.
    ns = report["namespace"]; e.require(re.fullmatch(r"iceberg\.dat03_e2e_[a-f0-9]{32}", ns), "CATALOG_CAPACITY_SCOPE_INVALID")
    commit = report["cases"][3]["first"]["commit"]
    before = commit["scope"]["baseline_snapshots"][table]; after = commit["tables"][table]["snapshot_id"]
    prior = report["cases"][2]["first"]["commit"]["tables"][table]
    e.require(table in TABLES and type(copies) is int and 1 <= copies <= 64
              and all(type(v) is int and 0 < v <= 9223372036854775807 for v in (before, after))
              and prior["snapshot_id"] == before and commit["scope"]["affected_months"] == ["2023-12"], "CATALOG_CAPACITY_PINS_INVALID")
    def side(pin, bucket):
        month = "format_datetime(e.event_time_utc AT TIME ZONE 'UTC','yyyy-MM')" if table == "event_current" else "e.event_month_utc"
        return (f"SELECT e.*, n.copy_id AS dat03_capacity_copy FROM {ns}.{table} FOR VERSION AS OF {pin} AS e "
                f"CROSS JOIN UNNEST(sequence(1,{copies})) AS n(copy_id) WHERE {month} <> '2023-12'"
                " AND lower(substr(to_hex(sha256(to_utf8(coalesce(e.canonical_event_id,'')))),1,1))='" + bucket + "'")
    return [f"SELECT count(*) AS changed_rows FROM (({side(before,b)} EXCEPT {side(after,b)}) UNION ALL ({side(after,b)} EXCEPT {side(before,b)})) changes"
            for b in "0123456789abcdef"]


def capacity(root, request, plan, summary):
    report = load(root / "bounded-report.json")
    # The profile upper bound includes every accepted JMA row in the split; the
    # validated USGS total deliberately includes halo duplicates. Not a canonical count.
    required = sum(s["profile_accepted_rows"] for s in plan["splits"].values()) + summary["sum_response_features_including_halo_duplicates"]
    e.require(type(required) is int and required > 0, "CATALOG_CAPACITY_SCALE_INVALID")
    e.require([p["table"] for p in request["trino_capacity"]] == list(TABLES), "CATALOG_TWO_CAPACITY_PROOFS_REQUIRED")
    for pin in request["trino_capacity"]:
        table = pin["table"]; q = root / f"capacity/{table}-request.json"; p = root / f"capacity/{table}-receipt.json"
        e.require(e.sha(q) == pin["request_sha256"] and e.sha(p) == pin["receipt_sha256"], "CATALOG_CAPACITY_BYTES_CHANGED")
        query, proof = load(q), load(p)
        e.require(query["version"] == proof["version"] == "dat03-trino-stress-v2" and query["table"] == proof["table"] == table
                  and query["strategy"] == proof["strategy"] == "buckets-v1" and query["query_max_run_time_seconds"] == 90
                  and type(query["total_probe_budget_seconds"]) is int and 600 <= query["total_probe_budget_seconds"] <= 3600
                  and query["report_sha256"] == e.sha(root / "bounded-report.json")
                  and query["queries"] == expected_queries(report, query["copies"], table)
                  and type(query["stress_rows_per_side"]) is int and query["stress_rows_per_side"] >= required
                  and query["stress_rows_per_side"] == report["cases"][2]["first"]["commit"]["tables"][table]["count"] * query["copies"],
                  "CATALOG_CAPACITY_WORKLOAD_INSUFFICIENT")
        e.require(proof["status"] == "STRESS_PROBE_PASSED" and proof["request_sha256"] == e.sha(q)
                  and proof["required_query_count"] == proof["completed_query_count"] == len(proof["queries"]) == 16
                  and proof["postflight_status"] == "PASSED_UNCHANGED" and proof["services_before"] == proof["services_after"]
                  and proof["data_mutations"] is False and proof["lease_acquired_or_released"] is False
                  and proof["full_capacity_accepted"] is False, "CATALOG_CAPACITY_NOT_ACCEPTED")
        for number, item in enumerate(proof["queries"]):
            output = load(root / f"capacity/{table}-{number:02d}-stdout.private", 16384)
            e.require(item["number"] == number and type(item["exit_code"]) is int and item["exit_code"] == 0
                      and item["status"] == "STRESS_PROBE_PASSED" and set(output) == {"changed_rows"}
                      and type(output["changed_rows"]) is int and output["changed_rows"] == 0, "CATALOG_CAPACITY_READBACK_CHANGED")
    return required


def validate(root):
    root = e.safe(root); request = load(root / "request.json", 65536)
    e.require(set(request) == {"version", "run_id", "namespace", "runner_sha256", "controller_sha256", "support_sha256", "source_cutoff_utc",
                              "baseline_inventory_sha256", "boundary_inventory_sha256", "plan_sha256", "usgs_summary_sha256", "sources", "trino_capacity"}
              and request["version"] == VERSION and re.fullmatch(r"dat03-catalog-[a-f0-9]{32}", request["run_id"])
              and request["namespace"] == "iceberg.dat03_catalog_" + request["run_id"][14:], "CATALOG_REQUEST_INVALID")
    for file, field in (("runner.jar", "runner_sha256"), ("controller.py", "controller_sha256"), ("research-e2e.py", "support_sha256"),
                        ("plan.json", "plan_sha256"), ("usgs-summary.json", "usgs_summary_sha256"),
                        ("baseline.csv", "baseline_inventory_sha256"), ("boundary.csv", "boundary_inventory_sha256")):
        e.require(e.sha(e.safe(root / file)) == request[field], "CATALOG_CODE_OR_CONFIG_PIN_CHANGED")
    plan = load(root / "plan.json"); summary = load(root / "usgs-summary.json")
    e.require(plan["version"] == "dat03-rollout-plan-v1" and plan["status"] == "PLAN_ONLY" and len(plan["batches"]) == 288
              and [b["month"] for b in plan["batches"]] == module_months()
              and plan["splits"]["REPRODUCTION"]["months"] == 225 and plan["splits"]["EXTENSION"]["months"] == 63,
              "CATALOG_EXACT_RESEARCH_PLAN_REQUIRED")
    e.require([p["name"] for p in request["sources"]] == list(NAMES), "CATALOG_EXACT_SOURCE_SET_REQUIRED")
    cutoff = datetime.fromisoformat(request["source_cutoff_utc"].replace("Z", "+00:00"))
    e.require(cutoff.tzinfo is not None and cutoff <= datetime.now(timezone.utc), "CATALOG_CUTOFF_INVALID")
    for pin in request["sources"]:
        meta_name, raw_name = source_files(pin["name"]); meta = load(root / meta_name, 65536); raw = e.safe(root / raw_name)
        e.require(e.sha(root / meta_name) == pin["metadata_sha256"] and raw.is_file() and type(pin["bytes"]) is int
                  and 0 < raw.stat().st_size == meta["bytes"] == pin["bytes"] <= 32*1024**2
                  and e.sha(raw) == meta["sha256"] == pin["raw_sha256"]
                  and datetime.fromisoformat(meta["retrieved_at_utc"].replace("Z", "+00:00")) <= cutoff, "CATALOG_SOURCE_PIN_CHANGED")
        if pin["name"].startswith("h"):
            expected = next(p for p in plan["sources"] if p["year"] == int(pin["name"][1:]))
            e.require(expected["archive_sha256"] == pin["raw_sha256"] and expected["capture_sha256"] == pin["metadata_sha256"]
                      and expected["bytes"] == pin["bytes"] and expected["catalog_release"] == meta["catalog_release"], "CATALOG_PROFILE_SOURCE_CHANGED")
        else:
            expected = next(p for p in summary["captures"] if p["month"] == pin["name"][5:])
            e.require(expected["raw_sha256"] == pin["raw_sha256"] and expected["capture_sha256"] == pin["metadata_sha256"]
                      and type(expected["check"]["feature_count"]) is int and 0 <= expected["check"]["feature_count"] < 20000, "CATALOG_USGS_SUMMARY_CHANGED")
    e.require(summary["sum_response_features_including_halo_duplicates"] == sum(p["check"]["feature_count"] for p in summary["captures"])
              and len(summary["captures"]) == 12, "CATALOG_USGS_SUMMARY_CHANGED")
    capacity(root, request, plan, summary)
    return request


def module_months():
    return [f"{y}-{m:02d}" for y in range(2000, 2024) for m in range(1, 13)]


def prepare(deployment, profiles, usgs, event_probe, bridge_probe):
    r = module("research-rollout"); u = module("capture-research-usgs")
    deployment, profiles, usgs = map(e.safe, (deployment, profiles, usgs))
    plan = r.plan(profiles); jar = e.safe(ROOT / "spark/target/japan-earthquake-etl-runner.jar")
    with zipfile.ZipFile(jar) as archive: e.require(CHECK.replace(".", "/") + ".class" in archive.namelist(), "CATALOG_BUILD_LOCAL_JAR_FIRST")
    summaries = list(usgs.glob("summary-*.json")); e.require(len(summaries) == 1, "CATALOG_EXACT_USGS_SUMMARY_REQUIRED")
    summary = load(summaries[0]); e.require(summary["capture_count"] == 12 and summary["status"] == "CAPTURES_STRUCTURE_VERIFIED", "CATALOG_USGS_SUMMARY_INVALID")
    bounded = load(ROOT / "docs/evidence/DAT-03_CONTINUATION.json")["checkpoint_recovery"]
    report = deployment / "staging/dat03-continuation" / bounded["attempt_id"] / "runtime/report.json"
    e.require(e.sha(report) == bounded["report_sha256"], "CATALOG_BOUNDED_REPORT_CHANGED")
    e.require(shutil.disk_usage(deployment).free >= 4*1024**3, "CATALOG_DISK_RESERVE")
    key = uuid.uuid4().hex; folder = e.safe(deployment / "staging/dat03-catalog" / key); folder.mkdir(parents=True, mode=0o700)
    (folder / "inputs").mkdir(mode=0o700); (folder / "capacity").mkdir(mode=0o700)
    def copy(source, name):
        source = e.safe(source); target = e.safe(folder / name)
        e.require(source.is_file() and 0 < source.stat().st_size <= 64*1024**2, "CATALOG_COPY_BOUND")
        with source.open("rb") as src, target.open("xb") as dst:
            os.chmod(target, 0o600); shutil.copyfileobj(src, dst, 1024**2)
        e.require(e.sha(source) == e.sha(target), "CATALOG_COPY_CHANGED")
    for source, name in ((jar, "runner.jar"), (Path(__file__), "controller.py"), (HERE / "research-e2e.py", "research-e2e.py"),
                         (ROOT / "config/jma/hypocenter_archives_v1.csv", "baseline.csv"),
                         (ROOT / "config/jma/research_boundary_archives_v1.csv", "boundary.csv"), (summaries[0], "usgs-summary.json"), (report, "bounded-report.json")):
        copy(source, name)
    e.save(folder / "plan.json", plan)
    request = {"version": VERSION, "run_id": "dat03-catalog-" + key, "namespace": "iceberg.dat03_catalog_" + key,
               "source_cutoff_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"), "sources": [], "trino_capacity": []}
    for name in NAMES:
        meta_name, raw_name = source_files(name)
        source = profiles / name if name.startswith("h") else usgs / name[5:]
        meta = load(source / "capture.json", 65536) if name.startswith("h") else u.verify_metadata(source, name[5:])
        copy(source / "capture.json", meta_name); copy(source / (name + ".zip" if name.startswith("h") else "response.geojson"), raw_name)
        request["sources"].append({"name": name, "raw_sha256": e.sha(folder / raw_name), "metadata_sha256": e.sha(folder / meta_name), "bytes": meta["bytes"]})
    for table, source in zip(TABLES, (event_probe, bridge_probe)):
        source = e.safe(source)
        for part in ("request", "receipt"): copy(source / (part + ".json"), f"capacity/{table}-{part}.json")
        for number in range(16): copy(source / f"{number:02d}-stdout.private", f"capacity/{table}-{number:02d}-stdout.private")
        request["trino_capacity"].append({"table": table, "request_sha256": e.sha(folder / f"capacity/{table}-request.json"),
                                         "receipt_sha256": e.sha(folder / f"capacity/{table}-receipt.json")})
    for file, field in (("runner.jar", "runner_sha256"), ("controller.py", "controller_sha256"), ("research-e2e.py", "support_sha256"),
                        ("plan.json", "plan_sha256"), ("usgs-summary.json", "usgs_summary_sha256"),
                        ("baseline.csv", "baseline_inventory_sha256"), ("boundary.csv", "boundary_inventory_sha256")):
        request[field] = e.sha(folder / file)
    e.save(folder / "request.json", request); validate(folder); return folder


def continuation_inputs(folder, control):
    folder, control = e.safe(folder), e.safe(control); base = validate(folder); request = load(control / "continuation-request.json", 65536)
    e.require(set(request) == {"version", "catalog_root", "catalog_request_sha256", "base_runner_sha256", "runner_sha256", "controller_sha256", "support_sha256", "compatibility_sha256"}
              and request["version"] == CONTINUATION_VERSION and re.fullmatch(r"/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}", request["catalog_root"])
              and Path(request["catalog_root"]).name == folder.name and request["catalog_request_sha256"] == e.sha(folder / "request.json")
              and request["base_runner_sha256"] == base["runner_sha256"], "CATALOG_CONTINUATION_PIN_CHANGED")
    for name, key in (("runner.jar", "runner_sha256"), ("controller.py", "controller_sha256"), ("research-e2e.py", "support_sha256"), ("compatibility.json", "compatibility_sha256")):
        e.require(e.sha(e.safe(control / name)) == request[key], "CATALOG_CONTINUATION_CODE_CHANGED")
    e.require(compatibility(folder / "runner.jar", control / "runner.jar") == load(control / "compatibility.json", 65536), "CATALOG_UNREVIEWED_DATA_PLANE_CHANGE")
    return request


def prepare_continuation(deployment, folder):
    deployment, folder = e.safe(deployment), e.safe(folder)
    relative = folder.relative_to(deployment / "staging/dat03-catalog"); e.require(re.fullmatch(r"[a-f0-9]{32}", str(relative)), "CATALOG_EXACT_HOST_FOLDER_REQUIRED")
    base = validate(folder); jar = e.safe(ROOT / "spark/target/japan-earthquake-etl-runner.jar")
    control = e.safe(deployment / "staging/dat03-catalog-continuation" / uuid.uuid4().hex); control.mkdir(parents=True, mode=0o700)
    # Only reviewed catalog classes enter the continuation. Concurrent ML work
    # remains in the local full build, never silently enters a pinned ETL runner.
    overlay_catalog(folder / "runner.jar",jar,control / "runner.jar")
    proof = compatibility(folder / "runner.jar",control / "runner.jar")
    for source, name in ((Path(__file__), "controller.py"), (HERE / "research-e2e.py", "research-e2e.py")):
        with e.safe(source).open("rb") as src, (control / name).open("xb") as dst:
            os.chmod(control / name, 0o600); shutil.copyfileobj(src, dst, 1024**2)
    e.save(control / "compatibility.json", proof)
    request = {"version": CONTINUATION_VERSION, "catalog_root": "/opt/pipeline/staging/dat03-catalog/" + str(relative),
               "catalog_request_sha256": e.sha(folder / "request.json"), "base_runner_sha256": base["runner_sha256"]}
    for name, key in (("runner.jar", "runner_sha256"), ("controller.py", "controller_sha256"), ("research-e2e.py", "support_sha256"), ("compatibility.json", "compatibility_sha256")):
        request[key] = e.sha(control / name)
    e.save(control / "continuation-request.json", request); continuation_inputs(folder, control); return control


def overlay_catalog(original,compiled,target):
    original_pin=e.sha(original);compiled_pin=e.sha(compiled)
    prefix="ie212/earthquake/spark/"
    changed={prefix+n+".class" for n in ("usgs/ResearchCatalogJob","usgs/ResearchCatalogState","orchestration/FullMonthBronzeReader",
             "orchestration/FullMonthStateResolver","orchestration/RealEtlAdapter","gold/GoldPublisher","gold/GoldVerificationPlan")}
    added={prefix+n+".class" for n in ("usgs/ResearchCatalogBatchPlan","usgs/ResearchCatalogCompatibility","orchestration/FullMonthBronzeReader$JmaStructureCache")}
    with zipfile.ZipFile(e.safe(original)) as base,zipfile.ZipFile(e.safe(compiled)) as build:
        e.require(len(base.namelist())==len(set(base.namelist())) and len(build.namelist())==len(set(build.namelist()))
                  and changed<=set(base.namelist()) and changed|added<=set(build.namelist()),"CATALOG_OVERLAY_CLASS_SET_REQUIRED")
        with zipfile.ZipFile(e.safe(target),"x") as result:
            for item in base.infolist():
                source=build if item.filename in changed else base
                e.require(source.getinfo(item.filename).file_size<=16*1024**2,"CATALOG_JAR_EXPANSION_BOUND")
                result.writestr(item,source.read(item.filename))
            for name in sorted(added):
                e.require(build.getinfo(name).file_size<=16*1024**2,"CATALOG_JAR_EXPANSION_BOUND")
                result.writestr(build.getinfo(name),build.read(name))
    e.require(e.safe(target).stat().st_size<=64*1024**2,"CATALOG_COMPATIBLE_JARS_REQUIRED")
    e.require(e.sha(original)==original_pin and e.sha(compiled)==compiled_pin,"CATALOG_INPUT_BUILD_CHANGED_DURING_OVERLAY")
    compatibility(original,target)


def quiet(owner=None):
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool
    held = Path(os.environ["SOURCE_GUARD_ROOT"]) / "owner.json"
    e.require((not held.exists()) if owner is None else (held.exists() and load(held, 65536) == owner), "CATALOG_LEASE_NOT_QUIET")
    engine = create_engine(os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"], poolclass=NullPool,
                           connect_args={"connect_timeout": 10, "options": "-c default_transaction_read_only=on -c statement_timeout=10000"})
    try:
        with engine.connect() as c:
            e.require(c.execute(text("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')")).scalar_one() == 0, "CATALOG_ACTIVE_DAGRUN")
    finally: engine.dispose()
    for proc in Path("/proc").iterdir():
        if proc.name.isdigit():
            try: command = (proc / "comm").read_text().strip()
            except FileNotFoundError: continue
            e.require(command != "java", "CATALOG_ACTIVE_JAVA")
    import urllib.request
    url = "http://" + os.environ["TRINO_HOST"] + ":" + os.environ["TRINO_INTERNAL_PORT"] + "/v1/query"
    with urllib.request.urlopen(urllib.request.Request(url, headers={"X-Trino-User": "dat03-catalog"}), timeout=15) as response:
        raw = response.read(4*1024**2+1)
    e.require(len(raw) <= 4*1024**2, "CATALOG_QUERY_METADATA_BOUND"); rows = json.loads(raw)
    e.require(isinstance(rows, list) and all(r["state"] in ("FINISHED", "FAILED") for r in rows), "CATALOG_ACTIVE_TRINO_QUERY")


def session_result(folder, request, session):
    result = load(session / "result.json")
    e.require(result["pending_month"] is None and result["request_identity_sha256"] == e.digest(request)
              and type(result["completed_months"]) is int and 0 <= result["completed_months"] <= 288
              and result["official_exported"] is False and result["research_ready"] is False
              and result["status"] == ("CATALOG_PUBLISHED" if result["completed_months"] == 288 else "CHECKPOINTED")
              and type(result["full_research_catalog"]) is bool and result["full_research_catalog"] == (result["completed_months"] == 288),
              "CATALOG_SESSION_RESULT_CHANGED")
    e.require([p["month"] for p in result["months"]] == module_months()[:result["completed_months"]], "CATALOG_CHECKPOINT_PREFIX_CHANGED")
    previous = None
    for checkpoint in result["months"]:
        root = e.safe(folder / "months" / checkpoint["month"])
        e.require(load(root / "checkpoint.json", 65536) == checkpoint and checkpoint["previous_checkpoint_sha256"] == previous
                  and checkpoint["request_identity_sha256"] == e.digest(request) and checkpoint["status"] == "PUBLISHED"
                  and checkpoint["record_sha256"] == e.sha(e.safe(root / "record.json")), "CATALOG_CHECKPOINT_CHAIN_CHANGED")
        record = load(root / "record.json")
        e.require(record["context"]["resolver_request"]["source_cutoff_utc"] == request["source_cutoff_utc"]
                  and record["context"]["resolver_request"]["namespace"] == request["namespace"]
                  and checkpoint["month"] in record["context"]["resolver_request"]["target_months"]
                  and set(record["context"]["resolver_request"]["target_months"]) <= {p["month"] for p in result["months"]}
                  and record["publication"]["published"] is True and record["publication"]["verify_status"] == "PASSED"
                  and record["commit"]["tables"] == record["publication"]["snapshot_bundle"]
                  and checkpoint["snapshots"] == {t: p["snapshot_id"] for t, p in record["commit"]["tables"].items()}, "CATALOG_CHECKPOINT_RECORD_CHANGED")
        previous = e.sha(root / "checkpoint.json")
    expected = result["months"][-1]["snapshots"] if result["months"] else {t: 0 for t in e.TABLES}
    e.require(set(expected) == e.TABLES and result["current_snapshots"] == expected
              and all(type(v) is int and 0 <= v <= 9223372036854775807 for v in expected.values()), "CATALOG_CURRENT_PIN_CHANGED")
    return result


def worker(folder, seconds, max_months, continuation=None):
    import resource
    folder = e.safe(folder); e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}", str(folder)), "CATALOG_EXACT_RUNTIME_REQUIRED")
    e.require(type(seconds) is int and 1800 <= seconds <= 6600 and type(max_months) is int and 1 <= max_months <= 288, "CATALOG_SESSION_LIMIT_INVALID")
    request = validate(folder); extra = continuation_inputs(folder, continuation) if continuation is not None else None; quiet()
    sessions = folder / "sessions"; sessions.mkdir(exist_ok=True, mode=0o700)
    for previous in sessions.iterdir():
        e.require(previous.is_dir() and not previous.is_symlink() and (previous / "acceptance.json").is_file(), "CATALOG_PREVIOUS_SESSION_REQUIRES_REVIEW")
        pin = load(previous / "acceptance.json", 65536); measurement = load(previous / "execution.json", 65536)
        e.require(pin["status"] == "CHECKPOINT_ACCEPTED" and pin["request_sha256"] == e.sha(folder / "request.json")
                  and pin["result_sha256"] == e.sha(previous / "result.json") and pin["execution_sha256"] == e.sha(previous / "execution.json")
                  and type(measurement["exit_code"]) is int and measurement["exit_code"] == 0 and measurement["timed_out"] is False
                  and pin["lease_release"] == {"status": "RELEASED", "dag_id": DAG, "run_id": request["run_id"]}, "CATALOG_PREVIOUS_ACCEPTANCE_CHANGED")
        session_result(folder, request, previous)
    deps = Path("/opt/spark/jobs/iceberg"); jars = [deps / n for n in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar")]
    e.require(all(j.is_file() for j in jars) and shutil.disk_usage(folder).free >= 4*1024**3, "CATALOG_DEPS_OR_DISK_RESERVE")
    session = sessions / uuid.uuid4().hex; session.mkdir(mode=0o700)
    e.save(session / "intent.json", {"version": VERSION, "request_sha256": e.sha(folder / "request.json"), "seconds": seconds, "max_months": max_months,
                                    "iceberg_dependencies": {j.name: e.sha(j) for j in jars},
                                    "continuation_request_sha256": e.sha(continuation / "continuation-request.json") if extra else None})
    sys.path.insert(0, "/opt/airflow/dags"); from source_run_guard import lease
    owner = {"dag_id": DAG, "run_id": request["run_id"]}; lease("acquire", owner)
    command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m", "--jars", ",".join(map(str, jars)),
               "--conf", "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m", "--class", CHECK,
               str((continuation if extra else folder) / "runner.jar"), str(folder / "request.json"), str(session), str(seconds), str(max_months)]
    if extra: command.append(str(continuation / "continuation-request.json"))
    started = time.monotonic(); free = shutil.disk_usage(folder).free; timed_out = False
    with (session / "stdout.private").open("xb") as out, (session / "stderr.private").open("xb") as err:
        os.chmod(session / "stdout.private", 0o600); os.chmod(session / "stderr.private", 0o600)
        child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        try: exit_code = child.wait(timeout=seconds+120)
        except subprocess.TimeoutExpired:
            timed_out = True; os.killpg(child.pid, signal.SIGTERM)
            try: exit_code = child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL); exit_code = child.wait(timeout=10)
    measurement = {"exit_code": exit_code, "timed_out": timed_out, "wall_seconds": round(time.monotonic()-started, 3),
                   "child_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss, "disk_free_bytes_before": free,
                   "disk_free_bytes_after": shutil.disk_usage(folder).free, "lease_retained_at_execution_end": True}
    e.save(session / "execution.json", measurement)
    e.require(not timed_out and exit_code == 0, "CATALOG_SESSION_FAILED_LEASE_RETAINED")
    result = session_result(folder, request, session)
    expected_runner = extra["runner_sha256"] if extra else request["runner_sha256"]
    e.require(result.get("session_runner_sha256", request["runner_sha256"]) == expected_runner, "CATALOG_SESSION_RUNNER_CHANGED")
    if extra: e.require(result["continuation_sha256"] == e.sha(continuation / "continuation-request.json"), "CATALOG_SESSION_RUNNER_CHANGED")
    quiet(owner); validate(folder)
    release = lease("release", owner); e.require(release == {"status": "RELEASED", **owner}, "CATALOG_OWN_RELEASE_UNCONFIRMED")
    e.save(session / "acceptance.json", {"version": VERSION, "status": "CHECKPOINT_ACCEPTED", "request_sha256": e.sha(folder / "request.json"),
                                        "result_sha256": e.sha(session / "result.json"), "execution_sha256": e.sha(session / "execution.json"),
                                        "lease_release": release, "research_ready": False, "official_exported": False})
    print("event=dat03_catalog_session_complete completed=" + str(result["completed_months"]) + " required=288 session=" + session.name, flush=True)


TRANSFER = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}",str(p))
assert not any(q.is_symlink() for q in (p,*p.parents))
header=sys.stdin.buffer.readline(65537);assert len(header)<=65536
files=json.loads(header);assert isinstance(files,list) and len(files)==119 and len({f["name"] for f in files})==119
expected={"runner.jar","controller.py","research-e2e.py","request.json","plan.json","usgs-summary.json","baseline.csv","boundary.csv","bounded-report.json"}
for name in ["h"+str(y) for y in range(2000,2025)]+["usgs-2023-%02d"%m for m in range(1,13)]:
    expected.add("inputs/"+name+".json");expected.add("inputs/"+name+(".zip" if name.startswith("h") else ".geojson"))
for table in ("event_current","event_source_bridge"):
    expected.update("capacity/"+table+"-"+part+".json" for part in ("request","receipt"))
    expected.update("capacity/"+table+"-%02d-stdout.private"%i for i in range(16))
assert {f["name"] for f in files}==expected
assert all(type(f["bytes"]) is int and 0<f["bytes"]<=64*1024**2 and re.fullmatch(r"[a-f0-9]{64}",f["sha256"]) for f in files)
assert sum(f["bytes"] for f in files)<=256*1024**2
p.parent.mkdir(parents=True,exist_ok=True);p.mkdir(mode=0o700);(p/"inputs").mkdir(mode=0o700);(p/"capacity").mkdir(mode=0o700)
for f in files:
    remaining=f["bytes"];h=hashlib.sha256()
    with (p/f["name"]).open("xb") as out:
        os.chmod(p/f["name"],0o600)
        while remaining:
            b=sys.stdin.buffer.read(min(remaining,1024**2));assert b;out.write(b);h.update(b);remaining-=len(b)
    assert h.hexdigest()==f["sha256"]
assert not sys.stdin.buffer.read(1)
print("event=dat03_catalog_exact_files_staged",flush=True)
'''

CONTINUATION_TRANSFER = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-catalog-continuation/[a-f0-9]{32}",str(p))
assert not any(q.is_symlink() for q in (p,*p.parents))
header=sys.stdin.buffer.readline(16385);assert len(header)<=16384
files=json.loads(header);expected={"runner.jar","controller.py","research-e2e.py","compatibility.json","continuation-request.json"}
assert isinstance(files,list) and len(files)==5 and {f["name"] for f in files}==expected
assert all(type(f["bytes"]) is int and 0<f["bytes"]<=64*1024**2 and re.fullmatch(r"[a-f0-9]{64}",f["sha256"]) for f in files)
assert sum(f["bytes"] for f in files)<=66*1024**2
p.parent.mkdir(parents=True,exist_ok=True);p.mkdir(mode=0o700)
for f in files:
    remaining=f["bytes"];h=hashlib.sha256()
    with (p/f["name"]).open("xb") as out:
        os.chmod(p/f["name"],0o600)
        while remaining:
            b=sys.stdin.buffer.read(min(remaining,1024**2));assert b;out.write(b);h.update(b);remaining-=len(b)
    assert h.hexdigest()==f["sha256"]
assert not sys.stdin.buffer.read(1)
print("event=dat03_catalog_continuation_staged",flush=True)
'''


def run(deployment, folder, seconds=6000, max_months=288, resume=False, continuation=None):
    deadline = time.monotonic() + 7200
    e.require(type(seconds) is int and 1800 <= seconds <= 6600 and type(max_months) is int and 1 <= max_months <= 288, "CATALOG_SESSION_LIMIT_INVALID")
    r = module("research-rollout"); deployment, folder = map(e.safe, (deployment, folder))
    relative = folder.relative_to(deployment / "staging/dat03-catalog")
    e.require(re.fullmatch(r"[a-f0-9]{32}", str(relative)), "CATALOG_EXACT_HOST_FOLDER_REQUIRED")
    request = validate(folder); extra = continuation_inputs(folder, continuation) if continuation is not None else None
    r.services(); r.idle(); r.trino_queries_idle()
    inside = "/opt/pipeline/staging/dat03-catalog/" + str(relative)
    if not resume:
        e.require(not (folder / "runtime").exists(), "CATALOG_EXISTING_RUNTIME_REQUIRES_EXPLICIT_RESUME")
        listing = [{"name": name, "bytes": e.safe(folder / name).stat().st_size, "sha256": e.sha(folder / name)} for name in sorted(files())]
        with subprocess.Popen(["docker", "exec", "-i", e.CONTAINER, "python3", "-c", TRANSFER, inside], stdin=subprocess.PIPE) as child:
            child.stdin.write((json.dumps(listing)+"\n").encode())
            for item in listing:
                with (folder / item["name"]).open("rb") as src: shutil.copyfileobj(src, child.stdin, 1024**2)
            child.stdin.close(); e.require(child.wait(timeout=120) == 0, "CATALOG_STAGE_FAILED")
    else:
        e.require((folder / "runtime").is_dir(), "CATALOG_RESUME_REQUIRES_RETAINED_RUNTIME")
    code_root = folder; code_inside = inside; bootstrap_request = "request.json"
    if extra:
        relative = e.safe(continuation).relative_to(deployment / "staging/dat03-catalog-continuation")
        e.require(re.fullmatch(r"[a-f0-9]{32}", str(relative)), "CATALOG_EXACT_CONTINUATION_FOLDER_REQUIRED")
        code_root = e.safe(continuation); code_inside = "/opt/pipeline/staging/dat03-catalog-continuation/" + str(relative); bootstrap_request = "continuation-request.json"
        names = ("runner.jar", "controller.py", "research-e2e.py", "compatibility.json", "continuation-request.json")
        listing = [{"name": name, "bytes": (code_root/name).stat().st_size, "sha256": e.sha(code_root/name)} for name in names]
        inspect = '''import hashlib,json,sys
from pathlib import Path
p=Path(sys.argv[1]);files=json.loads(sys.argv[2])
assert not any(q.is_symlink() for q in (p,*p.parents))
if not p.exists(): print("MISSING")
else:
    assert {q.name for q in p.iterdir()}=={f["name"] for f in files}
    for f in files:
        q=p/f["name"];assert not q.is_symlink() and q.is_file() and q.stat().st_size==f["bytes"]
        assert hashlib.file_digest(q.open("rb"),"sha256").hexdigest()==f["sha256"]
    print("VERIFIED_EXISTING")
'''
        checked=subprocess.run(["docker","exec",e.CONTAINER,"python3","-c",inspect,code_inside,json.dumps(listing)],check=True,capture_output=True,text=True,timeout=60)
        e.require(checked.stdout.strip() in ("MISSING","VERIFIED_EXISTING"),"CATALOG_CONTINUATION_STAGE_CHANGED")
        if checked.stdout.strip()=="MISSING":
            with subprocess.Popen(["docker", "exec", "-i", e.CONTAINER, "python3", "-c", CONTINUATION_TRANSFER, code_inside], stdin=subprocess.PIPE) as child:
                child.stdin.write((json.dumps(listing)+"\n").encode())
                for name in names:
                    with (code_root/name).open("rb") as src: shutil.copyfileobj(src, child.stdin, 1024**2)
                child.stdin.close(); e.require(child.wait(timeout=120)==0, "CATALOG_CONTINUATION_STAGE_FAILED")
    try:
        seconds = min(seconds, int(deadline-time.monotonic())-600)
        e.require(seconds >= 1800, "CATALOG_SESSION_BUDGET_SPENT_BEFORE_EXECUTION")
        bootstrap = '''import hashlib,runpy,sys
from pathlib import Path
p=Path(sys.argv[1]);expected=sys.argv[2:5];request_name=sys.argv[5]
for name,pin in zip((request_name,"controller.py","research-e2e.py"),expected):
    f=p/name
    assert not any(q.is_symlink() for q in (f,*f.parents)) and f.is_file()
    assert hashlib.file_digest(f.open("rb"),"sha256").hexdigest()==pin
sys.path.insert(0,str(p));sys.argv=[str(p/"controller.py"),*sys.argv[6:]]
runpy.run_path(str(p/"controller.py"),run_name="__main__")
'''
        pins = extra if extra else request
        command=["docker", "exec", e.CONTAINER, "python3", "-c", bootstrap, code_inside,
                 e.sha(code_root/bootstrap_request),pins["controller_sha256"],pins["support_sha256"],bootstrap_request, "worker", "--folder", inside,
                 "--seconds", str(seconds), "--max-months", str(max_months)]
        if extra: command += ["--continuation", code_inside]
        subprocess.run(command, check=True, timeout=seconds+300)
    finally:
        # Each host observation is new, including failures. Never overwrite an old snapshot.
        target = folder / ("runtime" if not (folder / "runtime").exists() else "runtime-observation-" + uuid.uuid4().hex)
        subprocess.run(["docker", "cp", e.CONTAINER + ":" + inside, str(target)], check=True, timeout=120)


def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("mode", choices=("prepare", "prepare-continuation", "run", "resume", "worker", "verify-inputs"))
    p.add_argument("--deployment-root", type=Path); p.add_argument("--folder", type=Path)
    p.add_argument("--profiles", type=Path, default=ROOT / "staging/dat03-research-profile-20261024")
    p.add_argument("--usgs", type=Path, default=ROOT / "staging/dat03-usgs-2023-20261025")
    p.add_argument("--event-probe", type=Path); p.add_argument("--bridge-probe", type=Path)
    p.add_argument("--continuation", type=Path)
    p.add_argument("--seconds", type=int, default=6000); p.add_argument("--max-months", type=int, default=288); a = p.parse_args()
    if a.mode == "prepare":
        e.require(all(x is not None for x in (a.deployment_root, a.event_probe, a.bridge_probe)), "CATALOG_PREPARATION_PATHS_REQUIRED")
        print(prepare(a.deployment_root, a.profiles, a.usgs, a.event_probe, a.bridge_probe))
    elif a.mode == "prepare-continuation":
        e.require(a.deployment_root is not None and a.folder is not None, "CATALOG_RUNTIME_PATHS_REQUIRED")
        print(prepare_continuation(a.deployment_root,a.folder))
    elif a.mode == "worker": worker(a.folder, a.seconds, a.max_months,a.continuation)
    elif a.mode in ("run", "resume"):
        e.require(a.deployment_root is not None and a.folder is not None, "CATALOG_RUNTIME_PATHS_REQUIRED")
        run(a.deployment_root, a.folder, a.seconds, a.max_months, a.mode == "resume",a.continuation)
    else: print("CATALOG_INPUTS_VERIFIED " + validate(a.folder)["run_id"])


if __name__ == "__main__": main()
