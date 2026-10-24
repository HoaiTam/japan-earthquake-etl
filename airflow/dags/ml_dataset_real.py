"""MLI-02 real control plane. Only Java reads records, computes features and exports.

No shell runner supplied by DagRun, no Python dataset identity, no mock promotion.
Failures retain the exact whole-run lease and private logs for operator recovery.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from resource_pilot import java_identity
from source_run_guard import lease

VERSION = "mli-02-real-v1"
DAG = "mli_02_build_dataset"
STEPS = ("wait_gold_published", "resolve_gold_snapshot", "audit_gold_input", "resolve_mc",
         "select_mainshocks", "build_candidate_windows", "build_spacetime_features",
         "validate_feature_snapshot", "export_parquet", "mark_dataset_exported")
STATUSES = ("VERIFIED_PUBLISHED", "SNAPSHOT_PINNED", "AUDIT_PASSED", "MC_RESOLVED",
            "MAINSHOCKS_SELECTED", "WINDOWS_BUILT", "FEATURES_BUILT",
            "FEATURE_SNAPSHOT_VALIDATED", "DATASET_EXPORTED", "DATASET_EXPORTED")
LIMIT = 65536
JAR = Path("/opt/spark/jobs/japan-earthquake-etl-runner.jar")
SPARK = Path("/opt/spark/bin/spark-submit")
ICEBERG = Path("/opt/spark/jobs/iceberg")


def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)


def payload(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode()


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            digest.update(block)
    return digest.hexdigest()


def safe(path):
    path = Path(path)
    require(path.is_absolute() and ".." not in path.parts
            and not any(p.is_symlink() for p in (path, *path.parents)), "MLI02_UNSAFE_PATH")
    return path


def read(path):
    path = safe(path)
    require(path.is_file() and path.stat().st_size <= LIMIT, "MLI02_BOUNDED_METADATA_REQUIRED")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "MLI02_DUPLICATE_JSON_KEY")
            result[key] = value
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=unique,
                      parse_constant=lambda _: require(False, "MLI02_NONFINITE_METADATA"))


def immutable(path, value):
    path = safe(path)
    data = payload(value)
    require(len(data) <= LIMIT, "MLI02_BOUNDED_METADATA_REQUIRED")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        require(path.read_bytes() == data, "MLI02_IMMUTABLE_CONFLICT")
        return
    # Atomic publication, not a partially-written canonical receipt on interruption.
    fd, temporary = tempfile.mkstemp(prefix=".pending-", dir=path.parent)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    try:
        os.link(temporary, path)
    except FileExistsError:
        require(path.read_bytes() == data, "MLI02_IMMUTABLE_CONFLICT")
    finally:
        Path(temporary).unlink()


def roots(env):
    return {"run_base": str(safe(env.get("ML_BUILD_STAGING_ROOT", "/opt/pipeline/staging/ml-build"))),
            "export_root": str(safe(env.get("ML_BUILD_EXPORT_ROOT", "/opt/pipeline/staging/ml-export"))),
            "registry_root": str(safe(env.get("MLI_DATASET_REGISTRY_ROOT", "/opt/pipeline/staging/ml-artifacts")))}


def prepared(env):
    require(env.get("ML_BUILD_MODE", "real") == "real", "MLI02_REAL_MODE_REQUIRED")
    expected = env.get("ML_BUILD_RUNNER_SHA256", "")
    require(re.fullmatch(r"[a-f0-9]{64}", expected) is not None, "MLI02_RUNNER_PIN_REQUIRED")
    require(JAR.is_file() and sha(JAR) == expected and SPARK.is_file(), "MLI02_RUNNER_NOT_PREPARED")
    jars = sorted(ICEBERG.glob("*.jar"))
    require({p.name for p in jars} == {"iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar"}
            and all(p.is_file() for p in jars), "MLI02_ICEBERG_NOT_PREPARED")
    require(re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", env.get("ML_BUILD_NAMESPACE", ""))
            is not None, "MLI02_NAMESPACE_REQUIRED")
    timeout = int(env.get("ML_BUILD_RUNNER_TIMEOUT_SECONDS", "1800"))
    require(1 <= timeout <= 7200, "MLI02_BOUNDED_TIMEOUT_REQUIRED")
    return jars, timeout


def invoke(request, env):
    jars, timeout = prepared(env)
    folder = safe(request["run_root"]) / "attempts"
    folder.mkdir(parents=True, exist_ok=True)
    attempt = Path(tempfile.mkdtemp(prefix=request["step"] + "-", dir=folder))
    immutable(attempt / "request.json", request)
    command = [str(SPARK), "--master", "local[1]", "--driver-memory", "768m",
               "--conf", "spark.driver.host=127.0.0.1", "--conf", "spark.driver.bindAddress=127.0.0.1",
               "--conf", "spark.ui.enabled=false", "--conf", "spark.sql.shuffle.partitions=4",
               "--jars", ",".join(map(str, jars)),
               "--class", "ie212.earthquake.spark.ml.DatasetBuildJob", str(JAR), str(attempt / "request.json")]
    with (attempt / "stdout.private").open("xb") as out, (attempt / "stderr.private").open("xb") as err:
        os.chmod(out.name, 0o600)
        os.chmod(err.name, 0o600)
        try:
            result = subprocess.run(command, env=java_identity(dict(env), attempt), stdout=out, stderr=err,
                                    timeout=timeout, shell=False, check=False)
        except subprocess.TimeoutExpired:
            immutable(attempt / "outcome.json", {"reason": "MLI02_TIMEOUT_LEASE_RETAINED"})
            raise RuntimeError("MLI02_TIMEOUT_LEASE_RETAINED") from None
    immutable(attempt / "outcome.json", {"exit_code": result.returncode})
    require(result.returncode == 0, "MLI02_RUNNER_FAILED_LEASE_RETAINED")
    return read(attempt / "receipt.json")


def resolve(raw, env, attempt):
    from ml_dataset_runtime import validate_dataset_config, verify_gold_published, canonical_json, VERSIONS
    conf = raw.get("conf", {})
    require(isinstance(conf, dict) and not conf.get("mock_pipeline"), "MLI02_REAL_CONFIG_REQUIRED")
    validate_dataset_config(conf)
    config = {key: conf[key] for key in (*VERSIONS, "dataset_split", "period_start_utc",
              "period_end_utc_exclusive", "observation_cutoff_utc", "filter_config")}
    canonical_json(conf)  # Private keys, paths and non-finite values fail before acquisition.
    publication = verify_gold_published(conf.get("publication_reference", {}))
    require(raw.get("dag_id", DAG) == DAG and isinstance(raw.get("run_id"), str)
            and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:+@/-]{0,255}", raw["run_id"])
            and ".." not in raw["run_id"] and not raw["run_id"].endswith("/"), "MLI02_EXACT_OWNER_REQUIRED")
    prepared(env)
    paths = roots(env)
    run = "mli02-" + hashlib.sha256((DAG + "|" + raw["run_id"]).encode()).hexdigest()[:40]
    request = {"contract_version": VERSION, "dag_id": DAG, "run_id": raw["run_id"], "build_run_id": run,
               "config": config, "publication_reference": publication, "ml_namespace": env["ML_BUILD_NAMESPACE"],
               "run_root": str(Path(paths.pop("run_base")) / run), **paths,
               "runner_sha256": env["ML_BUILD_RUNNER_SHA256"]}
    require(len(payload(request)) <= LIMIT - 4096, "MLI02_BOUNDED_METADATA_REQUIRED")
    owner = {"dag_id": DAG, "run_id": raw["run_id"]}
    target = Path(request["run_root"]) / "context.json"
    if target.exists():
        context = read(target)
        require(all(context.get(k) == v for k, v in request.items()), "MLI02_CONTEXT_CHANGED")
        lease("assert", owner, env)
        return context
    lease("acquire", owner, env)
    plan = invoke({**request, "step": "plan", "attempt": attempt}, env)
    require(re.fullmatch(r"ds_[a-f0-9]{64}", plan.get("dataset_id", "")) is not None
            and plan.get("identity_payload_sha256") == plan["dataset_id"][3:]
            and all(plan.get("gold_publication", {}).get(k) == publication[k] for k in (
                "gold_publication_id", "gold_snapshot_id", "gold_table_name", "physical_gold_table_name", "publication_bundle_sha256")),
            "MLI02_INVALID_PLAN")
    context = {**request, "plan": plan, "runtime_protocol": VERSION, "dataset_id": plan["dataset_id"],
               "lease_owner": owner}
    immutable(target, context)
    return context


def checked(context, env):
    require(env.get("ML_BUILD_MODE", "real") == "real", "MLI02_REAL_MODE_REQUIRED")
    require(context.get("runtime_protocol") == VERSION, "MLI02_REAL_CONTEXT_REQUIRED")
    path = safe(context["run_root"])
    require(read(path / "context.json") == context, "MLI02_CONTEXT_CHANGED")
    require(context["ml_namespace"] == env.get("ML_BUILD_NAMESPACE")
            and context["runner_sha256"] == env.get("ML_BUILD_RUNNER_SHA256"), "MLI02_DEPLOYMENT_CHANGED")
    expected = roots(env)
    require(context["export_root"] == expected["export_root"] and context["registry_root"] == expected["registry_root"]
            and path.parent == Path(expected["run_base"]), "MLI02_DEPLOYMENT_CHANGED")
    return path


def validate(receipt, context, step):
    require(receipt.get("contract_version") == VERSION and receipt.get("run_id") == context["run_id"]
            and receipt.get("step") == step and receipt.get("dataset_id") == context["dataset_id"]
            and receipt.get("identity_payload_sha256") == context["plan"]["identity_payload_sha256"]
            and receipt.get("plan_sha256") == hashlib.sha256(payload(context["plan"])).hexdigest()
            and receipt.get("gold_snapshot_id") == context["plan"]["gold_publication"]["gold_snapshot_id"]
            and receipt.get("status") == STATUSES[STEPS.index(step)], "MLI02_RECEIPT_CHANGED")


def execute(step, context, upstream, env, attempt):
    require(step in STEPS and type(attempt) is int and attempt > 0, "MLI02_EXACT_STEP_REQUIRED")
    path = checked(context, env)
    target = path / "receipts" / (step + ".json")
    # A completed mark can be retried after our successful release, never a foreign owner release.
    if step == STEPS[-1] and (path / "lease-release.json").exists():
        receipt = read(target)
        summary(context, receipt, env)
        return receipt
    lease("assert", context["lease_owner"], env)
    request = {**context, "step": step, "attempt": attempt}
    index = STEPS.index(step)
    if index:
        previous = path / "receipts" / (STEPS[index - 1] + ".json")
        value = read(previous)
        validate(value, context, STEPS[index - 1])
        require(value == upstream, "MLI02_UPSTREAM_CHANGED")
        request["previous_receipt_sha256"] = sha(previous)
    else:
        require(upstream is None, "MLI02_UNEXPECTED_UPSTREAM")
    if target.exists():
        receipt = read(target)
        validate(receipt, context, step)
    else:
        receipt = invoke(request, env)
        validate(receipt, context, step)
        require(receipt.get("attempt") == attempt, "MLI02_ATTEMPT_CHANGED")
        immutable(target, receipt)
    if step == STEPS[-1]:
        verify_handoff(context, receipt)
        lease("assert", context["lease_owner"], env)
        release = lease("release", context["lease_owner"], env)
        require(release == {"status": "RELEASED", **context["lease_owner"]}, "MLI02_RELEASE_UNCONFIRMED")
        immutable(path / "lease-release.json", release)
    return receipt


def verify_handoff(context, receipt):
    require(receipt.get("trusted_handoff_verified") is True and receipt.get("feature_row_count", 0) > 0
            and receipt.get("mainshock_count", 0) > 0 and receipt.get("invalid_feature_count") == 0
            and receipt.get("quality_gate_status") == "PASSED", "MLI02_HANDOFF_NOT_READY")
    registry = safe(context["registry_root"]) / (context["dataset_id"] + ".json")
    bundle = safe(context["export_root"]) / ("dataset_id=" + context["dataset_id"])
    for path, key in ((registry, "dataset_manifest_sha256"), (bundle / "dataset_manifest.json", "bundle_manifest_sha256"),
                      (bundle / "checksums.sha256", "checksums_sha256")):
        require(safe(path).is_file() and sha(path) == receipt.get(key), "MLI02_HANDOFF_CHECKSUM_CHANGED")
    # TrustedDatasetResolver in Java verified the registry's exact EXPORTED pins and
    # candidate lineage. Hash it here without deserializing rows into Python/XCom.
    # Public index covers all portable files, not just an untrusted self-supplied manifest.
    lines = (bundle / "checksums.sha256").read_text().splitlines()
    require(1 <= len(lines) <= 64, "MLI02_CHECKSUM_INDEX_INVALID")
    names = set()
    for line in lines:
        match = re.fullmatch(r"([a-f0-9]{64})  ([A-Za-z0-9_.-]+)", line)
        require(match is not None, "MLI02_CHECKSUM_INDEX_INVALID")
        digest, name = match.groups()
        require(name not in names and safe(bundle / name).is_file() and sha(bundle / name) == digest,
                "MLI02_BUNDLE_CHANGED")
        names.add(name)
    require({"dataset_manifest.json", "export-checksums.sha256"} <= names, "MLI02_CHECKSUM_INDEX_INVALID")
    require({p.name for p in bundle.iterdir()} == names | {"checksums.sha256"}, "MLI02_BUNDLE_FILE_SET_CHANGED")


def summary(context, final, env, *, persist=True):
    path = checked(context, env)
    validate(final, context, STEPS[-1])
    require(read(path / "receipts" / (STEPS[-1] + ".json")) == final, "MLI02_FINAL_RECEIPT_CHANGED")
    verify_handoff(context, final)
    require(read(path / "lease-release.json") == {"status": "RELEASED", **context["lease_owner"]}, "MLI02_RELEASE_UNCONFIRMED")
    held = safe(env.get("SOURCE_GUARD_ROOT", "/opt/pipeline/staging/source-guard")) / "owner.json"
    require(not held.exists() or read(held) != context["lease_owner"], "MLI02_LEASE_STILL_HELD")
    synthetic = context["config"]["filter_config"].get("synthetic_component_qa") is True
    result = {"task": "MLI-02", "status": "DATASET_EXPORTED", "run_id": context["run_id"],
              "dataset_id": context["dataset_id"], "gold_publication": context["plan"]["gold_publication"],
              "ml_namespace": context["ml_namespace"], "runner_sha256": context["runner_sha256"],
              "export": final, "local_handoff_verified": True, "synthetic_component_qa": synthetic,
              "scientific_approval": False, "research_ready": False,
              "colab_handoff": {"ready": False, "auto_triggered": False, "next_gate": "EXP-02 local validation"},
              "lease_released": True}
    if persist:
        immutable(path / "run-summary.json", result)
    else:
        require(read(path / "run-summary.json") == result, "MLI02_SUMMARY_CHANGED")
    return result
