"""Read-only Docker inspection plus opt-in, exact-key SEC-01 S3 probes."""
import json
import hashlib
import re
import subprocess
import selectors
import time
from pathlib import Path

LIMIT = 2 * 1024 * 1024
LOG_LIMIT = 512 * 1024 * 1024
REQUIRED = {"minio", "minio-init", "airflow-postgres", "airflow-init",
            "airflow-api-server", "airflow-scheduler", "airflow-dag-processor",
            "spark-master", "spark-worker", "iceberg-rest", "trino"}
ALLOWED_PORTS = {("minio", 9001), ("airflow-api-server", 8080),
                 ("spark-master", 8080), ("trino", 8080)}
SENSITIVE = re.compile(r"PASSWORD|SECRET|ACCESS_KEY|FERNET|JWT|SQL_ALCHEMY_CONN|ROOT_USER")


def call(root, command, timeout=30, input=None):
    result = subprocess.run(command, cwd=root, input=input, capture_output=True, timeout=timeout)
    if result.returncode or len(result.stdout) + len(result.stderr) > LIMIT:
        raise RuntimeError("SEC_RUNTIME_COMMAND_FAILED_OR_OUTPUT_GUARD")
    return result.stdout


def environment(container):
    return dict(row.split("=", 1) for row in container.get("Config", {}).get("Env", []) if "=" in row)


def scan_container_logs(root, container_id, scan_text, values, limit=LOG_LIMIT, timeout=120):
    """Scan ALL retained bytes in bounded lines, never retain or echo raw logs.

    No --tail/--since: the larger total budget is not a partial-log exemption.
    A too-large line, byte budget, timeout or Docker error remains incomplete.
    """
    result = {"bytes": 0, "sha256": None, "complete": False, "categories": []}
    digest = hashlib.sha256(); categories = set(); carry = b""
    deadline = time.monotonic() + timeout
    process = subprocess.Popen(["docker", "logs", container_id], cwd=root,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    def inspect(data):
        text = data.decode("utf-8", errors="ignore")
        categories.update(scan_text(text))
        if any(value in text for value in values):
            categories.add("configured_secret_in_log")
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError("SEC_RUNTIME_LOG_TIMEOUT")
                if not selector.select(min(remaining, 1)):
                    continue
                data = process.stdout.read1(65536)
                if not data:
                    break
                result["bytes"] += len(data); digest.update(data)
                if result["bytes"] > limit:
                    raise RuntimeError("SEC_RUNTIME_LOG_BYTE_GUARD")
                lines = (carry + data).split(b"\n"); carry = lines.pop()
                if len(carry) > LIMIT or any(len(line) > LIMIT for line in lines):
                    raise RuntimeError("SEC_RUNTIME_LOG_LINE_GUARD")
                for line in lines:
                    inspect(line)
            inspect(carry)
            if process.wait(timeout=max(.01, deadline-time.monotonic())) != 0:
                raise RuntimeError("SEC_RUNTIME_LOG_COMMAND_FAILED")
        result["complete"] = True; result["sha256"] = digest.hexdigest()
    except Exception:
        result["reason"] = "SEC_RUNTIME_LOG_COVERAGE_INCOMPLETE"
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(); process.stdout.close()
    result["categories"] = sorted(categories)
    return result

def valid_permission_evidence(evidence, context):
    if evidence.get("passed") is not True or not isinstance(evidence.get("checks"), list):
        return False
    checks = evidence["checks"]
    required = {"bronze_delete_denied", "bronze_still_readable", "anonymous_read_denied",
                "anonymous_list_denied", "outside_prefix_read_denied",
                "outside_prefix_list_denied", "outside_prefix_write_denied", "mutable_probe_cleanup"}
    required |= {"pipeline_" + operation + "_" + prefix for prefix in context["prefixes"]
                 for operation in ("read", "list")}
    if any(row.get("passed") is not True for row in checks) or {row.get("check") for row in checks} != required:
        return False
    if sum(row.get("check") == "mutable_probe_cleanup" for row in checks) != 2:
        return False
    if len(checks) != len(required) + 1:
        return False
    keys = evidence.get("created_keys", [])
    if len(keys) != 3 or evidence.get("retained_keys") != keys[:1]:
        return False
    for key, prefix in zip(keys, context["prefixes"]):
        if not re.fullmatch(re.escape(prefix) + r"/_security/sec-01-[0-9a-f]{32}/probe.txt", key):
            return False
    if len({key.rsplit("/_security/", 1)[1] for key in keys}) != 1:
        return False
    return evidence.get("payload_sha256") == hashlib.sha256(b"SEC-01 harmless scoped permission probe\n").hexdigest()


def inspect_container(service, container, expected, root_values):
    findings = []
    ports = []
    for target, bindings in (container.get("HostConfig", {}).get("PortBindings") or {}).items():
        for binding in bindings or []:
            ports.append({"service": service, "target": int(target.split("/")[0]),
                          "host_ip": binding["HostIp"], "published": binding["HostPort"]})
            if (service, int(target.split("/")[0])) not in ALLOWED_PORTS or binding["HostIp"] != "127.0.0.1":
                findings.append({"scope": "runtime", "location": service, "category": "unexpected_deployed_port"})
    desired = {(int(p["target"]), p.get("host_ip"), str(p["published"])) for p in expected.get("ports", [])}
    actual = {(p["target"], p["host_ip"], p["published"]) for p in ports}
    if desired != actual:
        findings.append({"scope": "runtime", "location": service, "category": "deployed_binding_config_mismatch"})
    if service == "trino":
        expected_mount = next((mount for mount in expected.get("volumes", [])
                               if mount.get("target") == "/etc/trino/log.properties"), None)
        mounted = [mount for mount in container.get("Mounts", [])
                   if mount.get("Destination") == "/etc/trino/log.properties"]
        if expected_mount is None or len(mounted) != 1 or mounted[0].get("RW") is not False or mounted[0].get("Source") != expected_mount.get("source"):
            findings.append({"scope": "runtime", "location": service, "category": "trino_logging_config_not_deployed"})
    env = environment(container)
    for key, value in expected.get("environment", {}).items():
        if SENSITIVE.search(key) and (not isinstance(value, str) or env.get(key) != value):
            findings.append({"scope": "runtime", "location": service, "category": "deployed_credential_config_mismatch", "key": key})
    if service not in ("minio", "minio-init"):
        if any(key.startswith("MINIO_ROOT_") for key in env) or any(
                actual_value == root_values[0] or
                (root_values[1] and root_values[1] in actual_value) for actual_value in env.values()):
            findings.append({"scope": "runtime", "location": service, "category": "root_credential_in_consumer"})
    return findings, ports


def audit_runtime(root, scan_text, permissions=False):
    root = Path(root).resolve()
    result = {"findings": [], "pending": [], "coverage": {}, "passed": False}
    compose = ["docker", "compose", "--env-file", str(root / ".env"), "-f", str(root / "compose.yaml"), "--profile", "*"]
    phase = "config"
    try:
        config = json.loads(call(root, compose + ["config", "--format", "json"]))
        phase = "container_inventory"
        ids = call(root, compose + ["ps", "--all", "--quiet"]).decode().split()
        if not ids or any(not re.fullmatch(r"[0-9a-f]{64}", item) for item in ids):
            raise RuntimeError("SEC_RUNTIME_CONTAINER_SCOPE_INVALID")
        phase = "container_inspection"
        containers = json.loads(call(root, ["docker", "inspect", *ids]))
        root_env = config["services"]["minio"]["environment"]
        root_values = [root_env["MINIO_ROOT_USER"], root_env["MINIO_ROOT_PASSWORD"]]
        values = sorted({value for settings in config["services"].values()
                         for key, value in settings.get("environment", {}).items()
                         if SENSITIVE.search(key) and isinstance(value, str) and len(value) >= 16})
        seen = set()
        ports = []
        logs = []
        pipeline = None
        for container in containers:
            phase = "container_scope"
            labels = container.get("Config", {}).get("Labels") or {}
            service = labels.get("com.docker.compose.service")
            if (labels.get("com.docker.compose.project") != config["name"]
                    or labels.get("com.docker.compose.project.working_dir") != str(root)
                    or service not in config["services"]
                    or labels.get("com.docker.compose.oneoff", "").lower() != "false"):
                raise RuntimeError("SEC_RUNTIME_CONTAINER_SCOPE_INVALID")
            if service in seen:
                raise RuntimeError("SEC_RUNTIME_DUPLICATE_SERVICE")
            seen.add(service)
            state = container["State"]
            if service in REQUIRED:
                if service.endswith("-init"):
                    healthy = state["Status"] == "exited" and state["ExitCode"] == 0
                else:
                    healthy = state["Status"] == "running" and state.get("Health", {}).get("Status") == "healthy"
                if not healthy:
                    result["pending"].append({"scope": "runtime", "location": service, "reason": "service_not_ready"})
            phase = "deployed_bindings_and_roles"
            problems, bound = inspect_container(service, container, config["services"][service], root_values)
            result["findings"].extend(problems); ports.extend(bound)
            # Docker sends many log drivers' records to stderr; capture both privately.
            phase = "retained_logs"
            logged = scan_container_logs(root, container["Id"], scan_text, values)
            if not logged["complete"]:
                result["pending"].append({"scope":"runtime", "location":service,
                                          "reason":"SEC_RUNTIME_LOG_COVERAGE_INCOMPLETE"})
            for category in logged["categories"]:
                result["findings"].append({"scope": "container_logs", "location": service, "category": category})
            logs.append({"service": service, **{k:logged[k] for k in ("bytes","sha256","complete")}})
            if service == "airflow-api-server":
                pipeline = container["Id"]
        for service in sorted(REQUIRED - seen):
            result["pending"].append({"scope": "runtime", "location": service, "reason": "service_missing"})
        result["coverage"] = {"services": sorted(seen), "deployed_ports": ports, "container_logs": logs,
                              "log_scope": "all_retained_streamed_512MiB_per_container_2MiB_line_120s_guard"}
        # Retained-log findings remain blockers, but do not prevent safe scoped
        # permission tests when deployment, role and health checks succeeded.
        deployment_findings = [item for item in result["findings"] if item["scope"] != "container_logs"]
        if permissions and not deployment_findings and not result["pending"] and pipeline:
            phase = "permission_probes"
            env = config["services"]["minio-init"]["environment"]
            context = {"endpoint": env["MINIO_ENDPOINT"], "bucket": env["DATA_BUCKET"],
                       "prefixes": [env["BRONZE_PREFIX"], env["SILVER_PREFIX"],
                                    env["WAREHOUSE_PATH"].removeprefix("s3://" + env["DATA_BUCKET"] + "/")]}
            if context["endpoint"] != "http://minio:9000" or any(
                    not re.fullmatch(r"[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*", p) for p in context["prefixes"]):
                raise RuntimeError("SEC_RUNTIME_PERMISSION_SCOPE_INVALID")
            if any(segment in (".", "..") for p in context["prefixes"] for segment in p.split("/")):
                raise RuntimeError("SEC_RUNTIME_PERMISSION_SCOPE_INVALID")
            code = (root / "scripts/security-s3-probe.py").read_text(encoding="utf-8")
            code += "\nrun(json.loads(" + repr(json.dumps(context)) + "))\n"
            evidence = json.loads(call(root, ["docker", "exec", "-i", pipeline, "python", "-"], timeout=120, input=code.encode()))
            if any(value in json.dumps(evidence) for value in values):
                raise RuntimeError("SEC_RUNTIME_PERMISSION_REPORT_LEAK")
            result["coverage"]["permissions"] = evidence
            if not valid_permission_evidence(evidence, context):
                result["findings"].append({"scope": "runtime", "category": "permission_probe_failed"})
        else:
            result["pending"].append({"scope": "runtime", "reason": "permission_probe_not_run"})
        result["passed"] = not result["findings"] and not result["pending"]
    except Exception as error:
        reason = str(error) if isinstance(error, RuntimeError) and re.fullmatch(r"SEC_[A-Z_]+", str(error)) else "SEC_RUNTIME_FAILED_REDACTED"
        result["pending"].append({"scope": "runtime", "reason": reason, "phase": phase, "failure_kind": type(error).__name__})
    return result
