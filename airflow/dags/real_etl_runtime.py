"""ORC-07 control plane only. Java owns Bronze/Silver/Iceberg and Trino data gates."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

from etl_pipeline_runtime import _json_bytes, _unique_json_object, _uri
from jma_backfill_runtime import _atomic_json
from source_run_guard import lease

VERSION = "orc-07-v1"
PHASES = ("readiness", "bronze", "silver", "gold", "verify", "publish")
TABLES = {"event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band"}


def require(value, reason):
    if not value:
        raise RuntimeError(reason)


def digest(value):
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def operation(dag, run):
    return "orc07-" + hashlib.sha256((dag + "|" + run).encode()).hexdigest()[:40]


def root(environment):
    return Path(environment.get("ORC07_STAGING_ROOT", "/opt/pipeline/staging/real-etl"))


def enabled(environment=None):
    mode = (os.environ if environment is None else environment).get("ORC07_DAILY_MODE", "disabled")
    require(mode in {"disabled", "real"}, "INVALID_ORC07_DAILY_MODE")
    return mode == "real"


def load_request(key, checksum, environment):
    require(isinstance(key, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,96}", key), "EXACT_REQUEST_KEY_REQUIRED")
    require(isinstance(checksum, str) and re.fullmatch(r"[a-f0-9]{64}", checksum), "EXACT_REQUEST_SHA_REQUIRED")
    folder = root(environment) / "requests"
    target = folder / (key + ".json")
    require(not folder.is_symlink() and not target.is_symlink() and target.is_file()
            and target.stat().st_size <= 60000, "PINNED_REQUEST_UNAVAILABLE")
    data = target.read_bytes()
    require(hashlib.sha256(data).hexdigest() == checksum, "REQUEST_PIN_CHANGED")
    return json.loads(data, object_pairs_hook=_unique_json_object)


def resolve(dag, run, key, checksum, start, end, environment=None, summary=None):
    env = os.environ if environment is None else environment
    require(isinstance(dag, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", dag)
            and isinstance(run, str) and 0 < len(run) <= 250 and not any(ord(c) < 32 for c in run), "EXACT_REAL_OWNER_REQUIRED")
    op = operation(dag, run); request = load_request(key, checksum, env)
    folder = root(env) / op; target = folder / "context.json"
    source_sha = digest(summary) if summary is not None else None
    selection = {"request_key": key, "request_sha256": checksum, "source_summary_sha256": source_sha,
                 "window_start_utc": start, "window_end_utc": end}
    if target.exists():
        require(json.loads((folder / "selection.json").read_text()) == selection, "REAL_CONTEXT_CHANGED")
        context = json.loads(target.read_text(), object_pairs_hook=_unique_json_object)
        validate_context(context)
        return context
    request = deepcopy(request); request["run_id"] = op; request["dag_id"] = dag
    if summary is not None:
        require(summary.get("status") == "SourcesReady" and summary.get("verified") is True
                and summary.get("run_id") == run, "REAL_SOURCE_READINESS_REQUIRED")
        u = summary["usgs"]; window = summary["usgs_context"]
        fresh = [{"source_system": "USGS", "manifest_uri": u["manifest_uri"], "manifest_sha256": u["manifest_sha256"],
                  "sha256": u["sha256"], "window_start_utc": window["window_start_utc"], "window_end_utc": window["window_end_utc"]}]
        fresh += [{"source_system": "JMA_BULLETIN", **{name: item[name] for name in
                   ("manifest_uri", "manifest_sha256", "sha256", "year", "segment", "catalog_release")}}
                  for item in summary["jma"]["archives"]]
        # Existing full-window pins supply baseline coverage; daily delta never replaces it.
        pins = {item["manifest_uri"]: item for item in request["bronze_inputs"]}
        for item in fresh:
            require(item["manifest_uri"] not in pins or pins[item["manifest_uri"]] == item, "DAILY_INPUT_PIN_CHANGED")
            pins[item["manifest_uri"]] = item
        request["bronze_inputs"] = [pins[uri] for uri in sorted(pins)]
        now = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
        request["processed_at_utc"] = now; request["source_cutoff_utc"] = now
    context = {"contract_version": VERSION, "dag_id": dag, "run_id": run, "operation_id": op, "mode": "real",
               "trigger": "daily" if summary is not None else "manual", "window_start_utc": start, "window_end_utc": end,
               "source_summary_sha256": source_sha, "resolver_request": request}
    context["context_sha256"] = digest(context); validate_context(context)
    require(not folder.is_symlink(), "UNSAFE_REAL_STAGING")
    _atomic_json(folder / "selection.json", selection); _atomic_json(target, context)
    return context


def validate_context(context):
    require(isinstance(context, dict) and context.get("contract_version") == VERSION and context.get("mode") == "real", "REAL_PROTOCOL_REQUIRED")
    require(context.get("operation_id") == operation(context["dag_id"], context["run_id"]), "OPERATION_ID_CHANGED")
    require(context.get("context_sha256") == digest({k: v for k, v in context.items() if k != "context_sha256"}), "REAL_CONTEXT_CHANGED")
    for name in ("window_start_utc", "window_end_utc"):
        require(isinstance(context[name], str) and context[name].endswith("Z"), "UTC_Z_REQUIRED")
    start, end = (datetime.fromisoformat(context[name].replace("Z", "+00:00")) for name in ("window_start_utc", "window_end_utc"))
    require(start < end <= datetime.now(timezone.utc), "CLOSED_EXECUTION_INTERVAL_REQUIRED")
    request = context["resolver_request"]
    required = {"resolver_version", "run_id", "dag_id", "processed_at_utc", "source_cutoff_utc", "target_months",
                "namespace", "config_version", "code_version", "baseline_snapshots", "baseline", "bronze_inputs"}
    require(isinstance(request, dict) and set(request) == required and isinstance(request["resolver_version"], str)
            and request["resolver_version"] in {"orc-06-v1", "orc-06-v2"}, "EXACT_RESOLVER_FIELDS_REQUIRED")
    require(isinstance(request["namespace"], str) and re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", request["namespace"]), "EXACT_NAMESPACE_REQUIRED")
    require(set(request["baseline_snapshots"]) == TABLES and all(type(v) is int and 0 <= v <= 9223372036854775807
            for v in request["baseline_snapshots"].values()), "SIX_BASELINES_REQUIRED")
    for name in ("code_version", "config_version"):
        require(isinstance(request[name], str) and re.fullmatch(r"[A-Za-z0-9._-]{1,128}", request[name]), "PINNED_VERSION_REQUIRED")
    require(isinstance(request["target_months"], list) and 1 <= len(request["target_months"]) <= 24
            and len(set(request["target_months"])) == len(request["target_months"])
            and all(isinstance(m, str) and re.fullmatch(r"[12][0-9]{3}-(0[1-9]|1[0-2])", m) for m in request["target_months"]), "EXACT_MONTHS_REQUIRED")
    for name in ("processed_at_utc", "source_cutoff_utc"):
        require(isinstance(request[name], str) and request[name].endswith("Z"), "UTC_Z_REQUIRED")
        datetime.fromisoformat(request[name].replace("Z", "+00:00"))
    baseline = request["baseline"]
    if baseline is not None:
        fields = {"run_id", "registry_sha256"} | ({"commit_key", "commit_sha256"} if any(request["baseline_snapshots"].values()) else set())
        require(isinstance(baseline, dict) and set(baseline) == fields, "EXACT_BASELINE_REQUIRED")
        require(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", str(baseline["run_id"]))
                and re.fullmatch(r"[a-f0-9]{64}", str(baseline["registry_sha256"])), "EXACT_BASELINE_REQUIRED")
        if "commit_key" in fields:
            require(re.fullmatch(r"gold-commits/[A-Za-z0-9_-]{1,128}/commit\.json", str(baseline["commit_key"]))
                    and re.fullmatch(r"[a-f0-9]{64}", str(baseline["commit_sha256"])), "EXACT_BASELINE_REQUIRED")
    require(isinstance(request["bronze_inputs"], list) and len(request["bronze_inputs"]) <= 64, "BOUNDED_EXACT_INPUTS_REQUIRED")
    for pin in request["bronze_inputs"]:
        fields = {"source_system", "manifest_uri", "manifest_sha256", "sha256"}
        source = pin.get("source_system")
        require(source in {"USGS", "JMA_BULLETIN"}, "TWO_SOURCE_POLICY_REQUIRED")
        fields |= {"window_start_utc", "window_end_utc"} if source == "USGS" else {"year", "segment", "catalog_release"}
        require(set(pin) == fields and all(re.fullmatch(r"[a-f0-9]{64}", str(pin[k])) for k in ("sha256", "manifest_sha256")), "EXACT_INPUT_PIN_REQUIRED")
        _uri(pin["manifest_uri"], "real")
        for name in fields - {"source_system", "manifest_uri", "manifest_sha256", "sha256"}:
            require(type(pin[name]) is int if name == "year" else isinstance(pin[name], str)
                    and re.fullmatch(r"[A-Za-z0-9._:+-]{1,128}", pin[name]), "SAFE_INPUT_METADATA_REQUIRED")
    require(request["run_id"] == context["operation_id"] and request["dag_id"] == context["dag_id"], "RESOLVER_CONTEXT_CHANGED")
    _json_bytes(context)


def validate_receipt(phase, context, upstream, result):
    fields = {"contract_version", "mode", "operation_id", "context_sha256", "phase", "status", "upstream_sha256",
              "artifact_uri", "artifact_sha256"}
    if phase == "silver": fields.add("counts")
    if PHASES.index(phase) >= 3: fields.add("snapshots")
    if phase == "publish": fields |= {"published", "publication_id", "publication_status"}
    require(isinstance(result, dict) and set(result) == fields, "INVALID_REAL_RECEIPT")
    require(all(result.get(k) == context[k] for k in ("contract_version", "mode", "operation_id", "context_sha256"))
            and result.get("phase") == phase and result.get("status") == "ok"
            and result.get("upstream_sha256") == (digest(upstream) if upstream is not None else None), "REAL_RECEIPT_CONTEXT_CHANGED")
    _uri(result["artifact_uri"], "real")
    require(re.fullmatch(r"[a-f0-9]{64}", str(result["artifact_sha256"])), "REAL_ARTIFACT_SHA_REQUIRED")
    if phase == "silver":
        require(result["counts"].get("balanced") is True, "REAL_RECONCILIATION_FAILED")
    if PHASES.index(phase) >= 3:
        require(isinstance(result["snapshots"], dict) and set(result["snapshots"]) == TABLES
                and all(type(value) is int and 0 < value <= 9223372036854775807 for value in result["snapshots"].values()), "SIX_COMMITTED_SNAPSHOTS_REQUIRED")
        if phase in {"verify", "publish"}:
            require(result["snapshots"] == upstream["snapshots"], "REAL_SNAPSHOT_CHANGED")
    if phase == "publish":
        require(result["published"] is True and result["publication_status"] == "Published"
                and re.fullmatch(r"pub_[a-f0-9]{64}", str(result["publication_id"])), "REAL_PUBLICATION_REQUIRED")
    _json_bytes(result)
    return result


def invoke(phase, context, upstream, environment):
    folder = root(environment) / context["operation_id"]
    request = {"contract_version": VERSION, "phase": phase, "run_context": context, "upstream": upstream}
    path = folder / (phase + "-input.json"); _atomic_json(path, request)
    spark = Path("/opt/spark/bin/spark-submit")
    require(spark.is_file(), "REAL_SPARK_RUNTIME_NOT_PREPARED")
    iceberg = sorted(Path("/opt/spark/jobs/iceberg").glob("*.jar"))
    require(len(iceberg) == 2, "REAL_ICEBERG_RUNTIME_NOT_PREPARED")
    command = [str(spark), "--master", "local[1]", "--driver-memory", "768m", "--conf", "spark.driver.bindAddress=127.0.0.1",
               "--conf", "spark.driver.host=127.0.0.1", "--conf", "spark.ui.enabled=false", "--jars", ",".join(map(str, iceberg)),
               "--class", "ie212.earthquake.spark.RealEtlPhaseJob", "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(path)]
    timeout = int(environment.get("ORC07_PHASE_TIMEOUT_SECONDS", "1800"))
    require(1 <= timeout <= 86400, "INVALID_REAL_TIMEOUT")
    with (folder / (phase + ".stdout.private")).open("w+b") as output, (folder / (phase + ".stderr.private")).open("wb") as error:
        os.chmod(output.name, 0o600); os.chmod(error.name, 0o600)
        completed = subprocess.run(command, cwd="/opt/pipeline", env=dict(environment), stdout=output, stderr=error,
                                   timeout=timeout, check=False, shell=False)
        require(completed.returncode == 0 and output.tell() <= 65536, "REAL_PHASE_FAILED_LEASE_RETAINED")
        output.seek(0)
        return json.load(output, object_pairs_hook=_unique_json_object)


def execute(context, environment=None):
    env = dict(os.environ if environment is None else environment); validate_context(context)
    owner = {"dag_id": context["dag_id"], "run_id": context["run_id"]}
    folder = root(env) / context["operation_id"]; upstream = None; receipts = []
    lease("assert", owner, env)
    _atomic_json(folder / "execution.json", {"state": "RUNNING", "context_sha256": context["context_sha256"]})
    try:
        for phase in PHASES:
            lease("assert", owner, env)
            result = validate_receipt(phase, context, upstream, invoke(phase, context, upstream, env))
            _atomic_json(folder / (phase + "-receipt.json"), result)
            receipts.append(result); upstream = result
        summary = {"contract_version": VERSION, "status": upstream["publication_status"], "published": upstream["published"],
                   "operation_id": context["operation_id"], "context_sha256": context["context_sha256"],
                   "publication_id": upstream["publication_id"], "snapshots": upstream["snapshots"], "receipts": receipts,
                   "verification_report_uri": receipts[4]["artifact_uri"], "publication_uri": receipts[5]["artifact_uri"]}
        _atomic_json(folder / "summary.json", summary)
        _atomic_json(folder / "execution.json", {"state": "COMPLETE", "context_sha256": context["context_sha256"], "summary_sha256": digest(summary)})
        return {k: v for k, v in summary.items() if k != "receipts"}
    except BaseException:
        _atomic_json(folder / "failure.json", {"status": "FAILED_LEASE_RETAINED", "phase": phase,
                     "context_sha256": context["context_sha256"], "reason": "REAL_ETL_INCOMPLETE"})
        _atomic_json(folder / "execution.json", {"state": "FAILED_LEASE_RETAINED", "context_sha256": context["context_sha256"]})
        raise RuntimeError("REAL_ETL_INCOMPLETE_LEASE_RETAINED") from None


def release(context, summary, environment=None):
    env = os.environ if environment is None else environment
    validate_context(context)
    stored = json.loads((root(env) / context["operation_id"] / "summary.json").read_text())
    state = json.loads((root(env) / context["operation_id"] / "execution.json").read_text())
    require(state == {"state": "COMPLETE", "context_sha256": context["context_sha256"], "summary_sha256": digest(stored)}, "REAL_OUTCOME_UNCONFIRMED_LEASE_RETAINED")
    require(summary == {k: v for k, v in stored.items() if k != "receipts"}, "REAL_COMPLETION_CHANGED")
    require(stored["published"] is True and stored["status"] == "Published" and len(stored["receipts"]) == 6, "REAL_COMPLETION_REQUIRED")
    return lease("release", {"dag_id": context["dag_id"], "run_id": context["run_id"]}, env)


def daily(plan, summary, environment=None):
    env = os.environ if environment is None else environment
    if not enabled(env): return {"status": "SourcesReady", "published": False}
    context = resolve("orc_02_daily_sources", plan["run_id"], env.get("ORC07_DAILY_REQUEST_KEY"), env.get("ORC07_DAILY_REQUEST_SHA256"),
                      plan["usgs"]["window_start_utc"], plan["usgs"]["window_end_utc"], env, summary)
    return {"context": context, "summary": execute(context, env)}
