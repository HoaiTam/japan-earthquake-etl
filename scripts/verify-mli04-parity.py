"""Read back official REPRODUCTION shards through pinned Iceberg snapshots."""
import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import re
from statistics import mean
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
import time

ROOT = Path(__file__).resolve().parents[1]
VERSION = "mli04-readback-v1"
ALGORITHMS = ("WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE")
PIN = re.compile(r"[0-9a-f]{64}")
IDENTITY = re.compile(r"[A-Za-z0-9_-]{1,256}")
FILES = {"experiment_config.json", "experiment_metrics.json", "memberships.parquet",
         "requirements-lock.txt", "sequence_summary.parquet"}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def doc(path, expected=None, *, lf=False):
    data = path.read_bytes()
    if lf:
        data = data.replace(b"\r\n", b"\n")
    if expected is not None:
        require(PIN.fullmatch(expected) and sha(data) == expected, "MLI04_PIN_MISMATCH")
    return json.loads(data), sha(data)


def official_sources(root=ROOT):
    base = root / "docs/evidence"
    report, report_sha = doc(base / "EXP-04_OFFICIAL_REPRODUCTION.json", lf=True)
    require(report["status"] == "PASSED" and report["frozen_rule_published"] is True
            and report["evaluation_scope"] == "OFFICIAL_RESEARCH", "MLI04_EXP04_NOT_ACCEPTED")
    dataset = dict(report["dataset"])
    require(dataset["dataset_split"] == "REPRODUCTION" and dataset["dataset_status"] == "EXPORTED",
            "MLI04_EXPECTED_EXPORTED_REPRODUCTION")
    dat03, _ = doc(base / "DAT-03_OFFICIAL_COLLECTIONS_20261115.json")
    lineage = {
        (row["dataset_manifest_sha256"], row["registry_snapshot_id"], row["candidate_snapshot_id"], row["mainshock_snapshot_id"])
        for session in dat03["accepted_build_and_rerun_sessions"] for row in [session["final_readback"]]
        if row.get("dataset_id") == dataset["dataset_id"] and row.get("exported") is True
        and row.get("bundle_manifest_sha256") == dataset["manifest_sha256"]
    }
    require(len(lineage) == 1, "MLI04_TRUSTED_REGISTRY_PINS_MISSING")
    dataset["registry_lineage"] = list(next(iter(lineage)))
    expected = {}
    for task, filename in (
        ("exp02", "EXP-02_REPRODUCTION_REPLAY_FOR_EXP04"),
        ("exp03", "EXP-03_OFFICIAL_REPRODUCTION"),
    ):
        cross = report[f"{task}_cross_verification"]
        require(cross["all_checks_passed"] is True, "MLI04_UPSTREAM_NOT_VERIFIED")
        source, _ = doc(base / (filename + ".json"), cross["report_sha256"], lf=True)
        for key in ("dataset_id", "collection_id", "manifest_sha256", "checksums_sha256", "gold_snapshot_id"):
            require(source["dataset"][key] == dataset[key], "MLI04_UPSTREAM_DATASET_MISMATCH")
        shards = source["shard_verifications"]
        if task == "exp03":
            resolution = source["multi_sequence_reconciliation"]
            dataset["adaptive_resolution"] = {
                "raw_post": resolution["raw_aftershocks"],
                "resolved_unique": resolution["resolved_unique_aftershocks"],
                "deduplicated_post": resolution["aftershocks_deduplicated_count"],
            }
            shards = {"HDBSCAN_ADAPTIVE": shards}
        for algo, by_shard in shards.items():
            for shard, pin in by_shard.items():
                rid = pin["experiment_run_id"]
                require(rid not in expected, "MLI04_DUPLICATE_RUN")
                expected[rid] = (algo, shard, source["code_version"], pin)
    require(dataset["shard_count"] == 2 and Counter((v[0], v[1]) for v in expected.values()) ==
            {(algo, shard): 1 for algo in ALGORITHMS for shard in ("shard_00000", "shard_00001")},
            "MLI04_INCOMPLETE_COHORT")
    return dataset, report_sha, expected


def snapshot(value):
    require(type(value) is int or isinstance(value, str) and re.fullmatch(r"[0-9]{1,19}", value),
            "MLI04_INVALID_SNAPSHOT")
    number = int(value)
    require(0 < number <= 2**63 - 1, "MLI04_INVALID_SNAPSHOT")
    return number


def bundle_expectations(root, pins, dataset, rid, algo, shard, code):
    import pyarrow.parquet as pq

    require(root.is_dir() and not root.is_symlink(), "MLI04_BUNDLE_PATH")
    require(not (root / "_SUCCESS.json").is_symlink(), "MLI04_BUNDLE_PATH")
    marker, _ = doc(root / "_SUCCESS.json", pins["success_marker_sha256"])
    checksums = marker["checksums"]
    require(checksums == pins["checksums"] and set(checksums) == FILES, "MLI04_BUNDLE_FILE_SET")
    for name, digest in checksums.items():
        path = root / name
        require(path.is_file() and not path.is_symlink() and sha(path.read_bytes()) == digest,
                "MLI04_BUNDLE_CHECKSUM")
    cfg, _ = doc(root / "experiment_config.json", checksums["experiment_config.json"])
    ds = dataset["dataset_id"]
    version = marker["model_config_version"]
    require(marker["status"] == "COMPLETED" and marker["experiment_run_id"] == rid
            and marker["algorithm_name"] == algo and marker["dataset_id"] == ds
            and marker["code_version"] == code and cfg["collection_scope"] == pins["collection_scope"]
            and cfg["collection_scope"]["shard_id"] == shard
            and cfg["collection_scope"]["collection_id"] == dataset["collection_id"], "MLI04_BUNDLE_IDENTITY")
    member = pq.read_table(root / "memberships.parquet",
                           columns=["candidate_event_id", "is_sequence_member", "event_role_candidate", "resolution_rank"])
    summary = pq.read_table(root / "sequence_summary.parquet").to_pylist()
    post = [r for r in member.to_pylist() if r["is_sequence_member"] and r["event_role_candidate"] == "POST"]
    statuses = Counter(r["window_result_status"] for r in summary)
    failures = Counter((r["window_result_status"], r["failure_reason_code"]) for r in summary
                       if r["window_result_status"] in {"FAILED", "SKIPPED"})
    require(all(reason for _, reason in failures), "MLI04_MISSING_FAILURE_REASON")
    metrics = {}
    for name in METRICS:
        values = [r[name] for r in summary if r[name] is not None]
        metrics[name] = {"count": len(values), "mean": mean(values) if values else None}
    resolved = {r["candidate_event_id"] for r in post if r["resolution_rank"] == 1}
    values = [len(member), len(summary), sum(r["candidate_count"] for r in summary),
              statuses["SUCCESS"], statuses["FAILED"], statuses["SKIPPED"],
              len(post), len(resolved)]
    require(values[:2] == [pins["membership_row_count"], pins["summary_row_count"]]
            and values[3] + values[4] + values[5] == len(summary), "MLI04_BUNDLE_COUNTS")
    # Java ResultBundleContract hashes the sorted map in its TreeMap representation.
    hash_map = "{" + ", ".join(f"{key}={checksums[key]}" for key in sorted(checksums)) + "}"
    fingerprint = sha("\n".join((ds, rid, algo, version, hash_map)).encode("utf-8"))
    return version, fingerprint, values, failures, metrics, resolved


METRICS = ("noise_ratio", "dbcv_score", "omori_p", "omori_fit_metric")
COUNTS = ("membership_rows", "summary_rows", "candidate_rows", "success_windows",
          "failure_windows", "skipped_windows", "raw_post", "resolved_unique")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def trino_query(base, sql):
    parsed = urlsplit(base)
    require(parsed.scheme in {"http", "https"} and parsed.hostname and not parsed.username
            and not parsed.password and not parsed.query and not parsed.fragment
            and parsed.path in {"", "/"}, "MLI04_TRINO_ENDPOINT")
    origin = (parsed.scheme, parsed.netloc)
    url = base.rstrip("/") + "/v1/statement"
    opener = build_opener(ProxyHandler({}), NoRedirect())
    headers = {"X-Trino-User": "mli04_readback", "X-Trino-Time-Zone": "UTC", "Content-Type": "text/plain"}
    data, query_id, rows = sql.encode(), None, []
    deadline = time.monotonic() + 90
    for _ in range(200):
        require(time.monotonic() < deadline, "MLI04_TRINO_TIMEOUT")
        with opener.open(Request(url, data=data, headers=headers), timeout=10) as response:
            raw = response.read(2_097_153)
        require(len(raw) <= 2_097_152, "MLI04_TRINO_RESPONSE_LIMIT")
        payload = json.loads(raw)
        require("error" not in payload and isinstance(payload.get("id"), str), "MLI04_TRINO_QUERY_FAILED")
        query_id = query_id or payload["id"]
        require(query_id == payload["id"], "MLI04_TRINO_QUERY_ID_CHANGED")
        rows.extend(payload.get("data", []))
        require(len(rows) <= 1000, "MLI04_TRINO_ROW_LIMIT")
        url = payload.get("nextUri")
        if not url:
            return query_id, rows
        follow = urlsplit(url)
        require((follow.scheme, follow.netloc) == origin and not follow.fragment
                and follow.path.startswith("/v1/statement/"), "MLI04_TRINO_NEXT_URI")
        data = None
    raise ValueError("MLI04_TRINO_PAGE_LIMIT")


def verify_shard(query, ns, dataset, rid, algo, shard, code, pin, artifact, receipt):
    require(IDENTITY.fullmatch(rid) and IDENTITY.fullmatch(dataset["dataset_id"])
            and re.fullmatch(r"iceberg\.ml(?:_qa_[a-z0-9_]+)?", ns), "MLI04_UNSAFE_QUERY_SCOPE")
    version, fingerprint, counts, failures, metrics, resolved = bundle_expectations(
        artifact, pin, dataset, rid, algo, shard, code)
    ds = dataset["dataset_id"]
    status = receipt.get("experiment_status")
    require(status in {"CANDIDATE", "APPROVED"} and receipt["namespace"] == ns
            and receipt["experiment_run_id"] == rid and receipt["dataset_id"] == ds
            and receipt["algorithm_name"] == algo and receipt["result_bundle_sha256"] == fingerprint
            and receipt["collection_scope"] == pin["collection_scope"]
            and receipt["readback"] == dict(zip(("sequence_membership", "sequence_summary", "experiment_run"),
                                                 [*counts[:2], 1])), "MLI04_RECEIPT_IDENTITY")
    pins = {key: snapshot(receipt[key]) for key in
            ("experiment_snapshot_id", "membership_snapshot_id", "summary_snapshot_id")}
    filt = f"experiment_run_id = '{rid}'"
    exp = f"{ns}.experiment_run FOR VERSION AS OF {pins['experiment_snapshot_id']}"
    mem = f"{ns}.sequence_membership FOR VERSION AS OF {pins['membership_snapshot_id']}"
    summ = f"{ns}.sequence_summary FOR VERSION AS OF {pins['summary_snapshot_id']}"

    q_registry, registry = query(f"""SELECT dataset_id, algorithm_name, model_config_version, code_version,
        experiment_status, result_bundle_sha256, membership_snapshot_id, summary_snapshot_id,
        membership_row_count, summary_row_count, dataset_manifest_sha256, dataset_registry_snapshot_id,
        candidate_source_snapshot_id, mainshock_source_snapshot_id FROM {exp} WHERE {filt}""")
    require(registry == [[ds, algo, version, code, status, fingerprint, pins["membership_snapshot_id"],
                          pins["summary_snapshot_id"], *counts[:2], *dataset["registry_lineage"]]],
            "MLI04_REGISTRY_MISMATCH")
    filt += f" AND dataset_id = '{ds}'"
    q_members, memberships = query(f"""SELECT count(*),
        count_if(is_sequence_member AND event_role_candidate = 'POST'),
        count(DISTINCT CASE WHEN is_sequence_member AND event_role_candidate = 'POST'
            AND resolution_rank = 1 THEN candidate_event_id END) FROM {mem} WHERE {filt}""")
    aggregates = ", ".join(f"count({key}), avg({key})" for key in METRICS)
    q_summary, summaries = query(f"""SELECT count(*), sum(candidate_count),
        count_if(window_result_status = 'SUCCESS'), count_if(window_result_status = 'FAILED'),
        count_if(window_result_status = 'SKIPPED'), {aggregates}
        FROM {summ} WHERE {filt}""")
    require(len(memberships) == len(summaries) == 1, "MLI04_AGGREGATE_REQUIRED")
    actual_counts = [memberships[0][0], *summaries[0][:5], *memberships[0][1:]]
    require(actual_counts == counts, "MLI04_COUNT_PARITY")
    actual_metrics = {}
    for i, (name, expected) in enumerate(metrics.items()):
        count, avg = summaries[0][5 + 2*i:7 + 2*i]
        match = (avg is None) if expected["mean"] is None else (
            type(avg) in {int, float} and math.isclose(avg, expected["mean"], rel_tol=1e-9, abs_tol=1e-9))
        require(count == expected["count"] and match, "MLI04_METRIC_PARITY")
        actual_metrics[name] = {"count": count, "mean": avg}
    q_fail, failure_rows = query(f"""SELECT window_result_status, failure_reason_code, count(*)
        FROM {summ} WHERE {filt} AND window_result_status IN ('FAILED', 'SKIPPED') GROUP BY 1, 2""")
    require({(state, reason): count for state, reason, count in failure_rows} == failures, "MLI04_FAILURE_PARITY")
    return {"experiment_run_id": rid, "algorithm_name": algo, "shard_id": shard, "experiment_status": status,
            "result_bundle_sha256": fingerprint, "success_marker_sha256": pin["success_marker_sha256"],
            "model_config_version": version, "code_version": code, "candidate_at_utc": receipt["candidate_at_utc"],
            "snapshots": pins, "query_ids": [q_registry, q_members, q_summary, q_fail],
            "expected_counts": dict(zip(COUNTS, counts)), "actual_counts": dict(zip(COUNTS, actual_counts)),
            "expected_metrics": metrics, "actual_metrics": actual_metrics,
            "failure_reasons": sorted(failure_rows, key=lambda r: (r[0], r[1] or ""))}, resolved


def rollups(verified, resolved, expected, ns, dataset_id, query):
    result = {}
    for algo in ALGORITHMS:
        runs = [r for r in verified if r["algorithm_name"] == algo]
        if not runs or len(runs) != sum(entry[0] == algo for entry in expected.values()):
            continue
        ids = [r["experiment_run_id"] for r in runs]
        selects = [
            f"SELECT candidate_event_id FROM {ns}.sequence_membership "
            f"FOR VERSION AS OF {r['snapshots']['membership_snapshot_id']} "
            f"WHERE dataset_id = '{dataset_id}' AND experiment_run_id = '{rid}' "
            "AND is_sequence_member AND event_role_candidate = 'POST' AND resolution_rank = 1"
            for rid, r in zip(ids, runs)
        ]
        query_id, rows = query("SELECT count(DISTINCT candidate_event_id) FROM (" +
                               " UNION ALL ".join(selects) + ") AS scoped_members")
        unique = len(set().union(*(resolved[rid] for rid in ids)))
        require(rows == [[unique]], "MLI04_CROSS_SHARD_PARITY")
        counts = {key: sum(r["actual_counts"][key] for r in runs) for key in COUNTS}
        counts["resolved_unique"] = unique
        metrics = {}
        for key in METRICS:
            values = [r["actual_metrics"][key] for r in runs]
            count = sum(v["count"] for v in values)
            total = sum(v["mean"] * v["count"] for v in values if v["mean"] is not None)
            metrics[key] = {"count": count, "mean": total / count if count else None}
        result[algo] = {"run_ids": sorted(ids), "counts": counts, "sql_metrics": metrics, "query_id": query_id}
    return result


def check_adaptive_resolution(counts, expected):
    raw = counts["raw_post"]
    unique = counts["resolved_unique"]
    observed = {"raw_post": raw, "resolved_unique": unique, "deduplicated_post": raw - unique}
    require(observed == expected, "MLI04_ADAPTIVE_RECONCILIATION")
    return observed


def collect(request, query, root=ROOT):
    dataset, sha04, expected = official_sources(root)
    require(request.get("schema_version") == "mli04-request-v1", "MLI04_REQUEST_VERSION")
    ns = request["namespace"]
    inputs = request["runs"]
    require(isinstance(inputs, list) and inputs, "MLI04_RUN_INPUTS_REQUIRED")
    ids = [entry["experiment_run_id"] for entry in inputs]
    require(len(set(ids)) == len(ids) and set(ids) <= expected.keys(), "MLI04_RUN_SCOPE_MISMATCH")
    verified, resolved = [], {}
    for entry in inputs:
        rid = entry["experiment_run_id"]
        receipt, digest = doc(Path(entry["receipt"]), entry["receipt_sha256"])
        algo, shard, code, pin = expected[rid]
        row, resolved[rid] = verify_shard(query, ns, dataset, rid, algo, shard, code, pin,
                                         Path(entry["artifact_root"]), receipt)
        verified.append(dict(row, receipt_sha256=digest))
    complete = set(ids) == expected.keys()
    aggregates = rollups(verified, resolved, expected, ns, dataset["dataset_id"], query)
    if "HDBSCAN_ADAPTIVE" in aggregates:
        adaptive = aggregates["HDBSCAN_ADAPTIVE"]
        adaptive["exp03_resolution"] = check_adaptive_resolution(adaptive["counts"], dataset["adaptive_resolution"])
    return {"schema_version": VERSION, "task_id": "MLI-04", "status": "VERIFIED" if complete else "PENDING",
            "dataset_id": dataset["dataset_id"], "collection_id": dataset["collection_id"], "namespace": ns,
            "registry_lineage": dataset["registry_lineage"],
            "exp04_sha256_lf": sha04, "scientific_approval": False, "runs": verified,
            "algorithm_rollups": aggregates,
            "missing_run_ids": sorted(expected.keys() - set(ids))}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        request, _ = doc(args.request, args.request_sha256)
        endpoint = os.environ["MLI04_TRINO_URL"]
        result = collect(request, lambda sql: trino_query(endpoint, sql))
        with args.output.open("x", encoding="utf-8") as out:
            json.dump(result, out, ensure_ascii=False, indent=2, allow_nan=False)
            out.write("\n")
    except (OSError, TypeError, ValueError, KeyError, IndexError) as exc:
        reason = str(exc)
        parser.exit(2, (reason if re.fullmatch(r"MLI04_[A-Z_]+", reason) else "MLI04_READBACK_FAILED") + "\n")


if __name__ == "__main__":
    main()
