#!/usr/bin/env python3
"""SEC-01 redacted audit; optional exact-key QA writes. Exit 2 is incomplete."""
import argparse
import hashlib
import importlib.util
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import security_handoff

PATTERNS = {
    "aws_access_key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "github_token": re.compile(r"gh[pousr]_[A-Za-z0-9]{36,}"),
    "google_api_key": re.compile(r"AIza[0-9A-Za-z_-]{35}"),
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "credential_uri": re.compile(
        r"(?:https?|postgres(?:ql)?|s3)://"
        r"(?P<username>[^\s/:@]+):(?P<password>[^\s/@]+)@"
    ),
}
ML_PATTERNS = {
    "personal_account_path": re.compile(r"(?i)(?:[a-z]:[\\/]Users[\\/]|/Users/|/home/(?!iceberg(?:/|\b)))"),
    "credential_query": re.compile(r"(?i)[?&](?:access_token|token|password|secret|signature)=[^\s&]+"),
}
MAX_BYTES = 2 * 1024 * 1024
PLACEHOLDER_PASSWORD = re.compile(r"\A(?:\$\{[A-Za-z_][A-Za-z0-9_]*\}|change-me-[^\s/@]+)\Z")

def configured_secrets(root):
    path = root / ".env"
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_BYTES:
        return []
    keys = {"MINIO_ROOT_PASSWORD", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY",
            "AIRFLOW_ADMIN_PASSWORD", "AIRFLOW_FERNET_KEY", "AIRFLOW_API_JWT_SECRET",
            "AIRFLOW_DB_PASSWORD"}
    values = []
    for line in path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition("=")
        if separator and key in keys:
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            if len(value) >= 16 and not value.startswith("change-me-"):
                values.append(value)
    return values

def is_ml_artifact(name):
    return name.endswith(".ipynb") or any(part in name.split("/")
        for part in ("ml-export", "ml-pilot", "ml-artifacts", "experiments", "notebooks")) or "ml/results/" in name


def triage(categories, data, allowlist):
    checksum = hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()
    # Only exact, reviewed synthetic URI fixtures can be exempted; never keys or token patterns.
    known = []
    for category in categories:
        if category != "credential_uri":
            continue
        for item in allowlist:
            if item["category"] == category and item["normalized_sha256"] == checksum:
                known.append(category)
                break
    remaining = [category for category in categories if category not in known]
    return remaining, known


def scan_text(text, ml=False):
    categories = []
    for category, pattern in {**PATTERNS, **(ML_PATTERNS if ml else {})}.items():
        for match in pattern.finditer(text):
            # Only a complete password placeholder is exempt. Username markers and
            # substrings such as example/fixture do not make a real password safe.
            if category == "credential_uri" and PLACEHOLDER_PASSWORD.fullmatch(match.group("password")):
                continue
            categories.append(category)
            break
    return categories


def inspect_compose(config):
    findings = []
    published = []
    allowed = {("minio", 9001), ("airflow-api-server", 8080), ("spark-master", 8080), ("trino", 8080)}
    for service, settings in config.get("services", {}).items():
        for port in settings.get("ports", []):
            if not isinstance(port, dict):
                findings.append({"scope": "compose", "location": service, "category": "unsupported_port_format"})
                continue
            target = port.get("target")
            published.append({"service": service, "target": target, "host_ip": port.get("host_ip")})
            if (service, target) not in allowed or port.get("host_ip") != "127.0.0.1":
                findings.append({"scope": "compose", "location": service, "category": "unexpected_port_exposure"})
        environment = settings.get("environment", {})
        if service not in ("minio", "minio-init") and any(key.startswith("MINIO_ROOT_") for key in environment):
            findings.append({"scope": "compose", "location": service, "category": "root_credential_consumer"})
    return findings, published


def git(root, *args):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True, check=False)
    if result.returncode:
        raise RuntimeError("project_git_command_failed")
    return result.stdout


def deployment_root(source, deployment):
    """An explicit sibling worktree is allowed; unrelated projects never are."""
    if any(p.is_symlink() for p in (deployment,*deployment.parents)):
        raise RuntimeError("SEC_RUNTIME_UNRELATED_CHECKOUT")
    source = source.resolve(); deployment = deployment.resolve()
    a = git(source,"rev-parse","--path-format=absolute","--git-common-dir").decode().strip()
    b = git(deployment,"rev-parse","--path-format=absolute","--git-common-dir").decode().strip()
    if not a or a != b or any(p.is_symlink() for p in (deployment,*deployment.parents)):
        raise RuntimeError("SEC_RUNTIME_UNRELATED_CHECKOUT")
    return deployment


def audit(root, include_history=True, runtime=False, permissions=False, deployed_root=None, handoff=None):
    root = root.resolve()
    deployed = deployment_root(root,Path(deployed_root)) if deployed_root else root
    if handoff is not None:
        candidate = Path(handoff)
        if (any(p.is_symlink() for p in (candidate,*candidate.parents))
                or not any(base in candidate.resolve().parents for base in (root,deployed))):
            raise RuntimeError("SEC_HANDOFF_MANIFEST_OUTSIDE_PROJECT")
    report = {"schema_version": "1.1", "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "coverage": {}, "findings": [], "triaged_synthetic_fixtures": [], "pending": [], "deferred": [], "can_mark_done": False}
    report["coverage"]["deployment_git_head"] = git(deployed,"rev-parse","HEAD").decode().strip()
    secrets = configured_secrets(deployed)
    allow_path = root / "docs/security/SEC-01-synthetic-uris.json"
    allowlist = json.loads(allow_path.read_text(encoding="utf-8")) if allow_path.is_file() else []
    def record(scope, location, categories, data=None):
        if data is not None and scope in ("worktree", "history") and location != ".env":
            if any(value.encode() in data for value in secrets):
                categories = [*categories, "configured_secret_in_content"]
        if data is not None and scope in ("worktree", "history"):
            categories, known = triage(categories, data, allowlist)
            for category in known:
                report["triaged_synthetic_fixtures"].append({"scope": scope, "location": location, "category": category, "reason": "exact_reviewed_test_content"})
        for category in categories:
            report["findings"].append({"scope": scope, "location": location, "category": category})
    paths = git(root, "ls-files", "-co", "--exclude-standard", "-z").decode("utf-8").split("\0")
    current = 0
    for name in sorted(set(paths) - {""}):
        path = root / name
        if not path.is_file():
            continue
        if path.is_symlink() or root not in path.resolve().parents:
            record("worktree", name, ["unsafe_audit_path"])
            continue
        if path.stat().st_size > MAX_BYTES:
            report["pending"].append({"scope": "worktree", "location": name, "reason": "file_size_guard"})
            continue
        data = path.read_bytes()
        record("worktree", name, scan_text(data.decode("utf-8", errors="ignore"), ml=is_ml_artifact(name)), data)
        current += 1
    report["coverage"]["worktree_files"] = current
    env = deployed / ".env"
    for checkout in {root,deployed}:
        if git(checkout, "ls-files", ".env").strip():
            record("config", ".env", ["tracked_local_env"])
        ignored = subprocess.run(["git", "-C", str(checkout), "check-ignore", "-q", ".env"], capture_output=True).returncode == 0
        if not ignored:
            record("config", ".env", ["local_env_not_ignored"])
    if env.exists():
        if env.is_symlink() or env.resolve().parent != deployed:
            record("config", ".env", ["unsafe_audit_path"])
        elif env.stat().st_size > MAX_BYTES:
            report["pending"].append({"scope": "config", "location": ".env", "reason": "file_size_guard"})
        else:
            record("config", ".env", scan_text(env.read_text(encoding="utf-8", errors="ignore")))
            report["pending"].append({"scope": "config", "reason": "run_check_config_require_local_for_strength_and_role_checks"})
    else:
        report["pending"].append({"scope": "config", "reason": "local_env_absent"})
    if include_history:
        objects = sorted({line.split()[0] for line in git(root, "rev-list", "--objects", "--all").decode().splitlines()})
        metadata = subprocess.run(["git", "-C", str(root), "cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
            input=("\n".join(objects)+"\n").encode(), capture_output=True, check=True).stdout.decode().splitlines()
        blobs = []
        for line in metadata:
            oid, kind, size = line.split()
            if kind != "blob":
                continue
            if int(size) > MAX_BYTES:
                report["pending"].append({"scope": "history", "location": oid, "reason": "blob_size_guard"})
            else:
                blobs.append(oid)
        process = subprocess.Popen(["git", "-C", str(root), "cat-file", "--batch"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            for oid in blobs:
                process.stdin.write((oid+"\n").encode()); process.stdin.flush()
                header = process.stdout.readline().decode().split(); size = int(header[2])
                data = process.stdout.read(size); process.stdout.read(1)
                record("history", oid, scan_text(data.decode("utf-8", errors="ignore")), data)
        finally:
            process.stdin.close(); process.stdout.close(); process.wait()
        if process.returncode:
            raise RuntimeError("history_blob_reader_failed")
        report["coverage"]["reachable_git_blobs"] = len(blobs)
        report["coverage"]["git_head"] = git(root, "rev-parse", "HEAD").decode().strip()
    else:
        report["pending"].append({"scope": "history", "reason": "history_not_requested"})
    log_count = 0
    for directory in (deployed / "airflow/logs", deployed / "staging"):
        if not directory.exists():
            continue
        for path in directory.rglob("*.log"):
            if "m2" in path.relative_to(deployed).parts:
                continue
            name = path.relative_to(deployed).as_posix()
            if path.is_symlink() or deployed not in path.resolve().parents:
                record("logs", name, ["unsafe_audit_path"]); continue
            if path.stat().st_size > MAX_BYTES:
                report["pending"].append({"scope": "logs", "location": name, "reason": "file_size_guard"}); continue
            text = path.read_bytes().decode("utf-8", errors="ignore")
            categories = scan_text(text, ml=is_ml_artifact(name))
            if any(value in text for value in secrets):
                categories.append("configured_secret_in_log")
            record("logs", name, categories)
            log_count += 1
    report["coverage"]["local_log_files"] = log_count
    # Resolve type-checked ports using public placeholders, not --no-interpolate
    # (Compose 2.38 rejects uninterpolated published-port values).
    command = ["docker", "compose", "--env-file", str(root / ".env.example"),
               "-f", str(root / "compose.yaml"), "--profile", "*", "config",
               "--no-env-resolution", "--format", "json"]
    try:
        result = subprocess.run(command, cwd=root, capture_output=True, timeout=30)
        if result.returncode:
            report["pending"].append({"scope": "compose", "reason": "unresolved_config_check_failed"})
        else:
            findings, ports = inspect_compose(json.loads(result.stdout))
            report["findings"].extend(findings); report["coverage"]["declared_ports"] = ports
    except (OSError, subprocess.TimeoutExpired, ValueError):
        report["pending"].append({"scope": "compose", "reason": "compose_cli_unavailable"})
    if runtime and env.is_file() and not env.is_symlink() and env.stat().st_size <= MAX_BYTES:
        checked = subprocess.run([str(deployed / "scripts/check-config.sh"), "--require-local"],
                                 cwd=deployed, capture_output=True, timeout=30)
        if checked.returncode:
            report["findings"].append({"scope": "config", "category": "local_config_validation_failed"})
        else:
            report["pending"] = [item for item in report["pending"]
                                 if item.get("reason") != "run_check_config_require_local_for_strength_and_role_checks"]
            report["coverage"]["local_config_strength_and_roles"] = True
        module_spec = importlib.util.spec_from_file_location("sec_runtime", root / "scripts/security-runtime.py")
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        measured = module.audit_runtime(deployed, scan_text, permissions=(permissions and checked.returncode == 0
                                        and not report["findings"] and not report["pending"]))
        report["findings"].extend(measured["findings"])
        report["pending"].extend(measured["pending"])
        report["coverage"]["runtime"] = measured["coverage"]
    else:
        report["pending"].append({"scope": "runtime", "reason": "runtime_config_unavailable_or_unsafe"
                                  if runtime else "live_runtime_audit_not_requested"})
    # Current artifacts are scanned; absence is downstream work, not certification
    # of notebooks/exports that do not yet exist.
    report["coverage"]["current_baseline_passed"] = runtime and permissions and not report["findings"] and not report["pending"]
    handoff_report = security_handoff.audit(deployed,handoff,scan_text,secrets)
    report["coverage"]["ml_handoff"] = handoff_report
    report["findings"].extend(handoff_report["findings"])
    report["pending"].extend(handoff_report["pending"])
    report["deferred"] = [{"task_id":task,"reason":"re_audit_when_handoff_bytes_or_remote_permissions_change"}
                          for task in ("DAT-02","EXP-01","MLD-05","MLI-03")]
    report["can_mark_done"] = (runtime and permissions and handoff_report["passed"] is True
                                and not report["findings"] and not report["pending"])
    report["coverage"]["pattern_policy"] = "high_confidence_tokens_keys_and_credential_URIs_not_complete_secret_detection"
    report["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--runtime", action="store_true", help="inspect deployed ports, roles and retained container logs")
    parser.add_argument("--permission-probes", action="store_true", help="opt-in exact-key QA writes; requires --runtime")
    parser.add_argument("--deployment-root", type=Path, help="explicit linked checkout owning the deployed Compose project")
    parser.add_argument("--handoff-manifest", type=Path, help="pinned explicit local notebook/pilot/export/result artifacts")
    args = parser.parse_args()
    if args.permission_probes and not args.runtime:
        parser.error("--permission-probes requires --runtime")
    root = Path(__file__).resolve().parent.parent
    target = args.output.resolve()
    if root not in target.parents or args.output.is_symlink():
        parser.error("output must stay inside project and not be a symlink")
    try:
        report = audit(root, runtime=args.runtime, permissions=args.permission_probes,
                       deployed_root=args.deployment_root, handoff=args.handoff_manifest)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2, ensure_ascii=False)+"\n", encoding="utf-8")
    except Exception:
        print("SEC-01 audit failed; diagnostic values redacted")
        return 1
    print("SEC-01 scoped audit:", len(report["findings"]), "findings;", len(report["pending"]),
          "pending coverage items; can_mark_done=" + str(report["can_mark_done"]).lower())
    return 1 if report["findings"] else (0 if report["can_mark_done"] else 2)


if __name__ == "__main__":
    raise SystemExit(main())
