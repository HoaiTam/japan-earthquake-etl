#!/usr/bin/env python3
"""SEC-01 scoped, read-only, redacted audit. Exit 2 means coverage incomplete."""
import argparse
import hashlib
import json
import re
import subprocess
from pathlib import Path

PATTERNS = {
    "aws_access_key": re.compile(r"AKIA[0-9A-Z]{16}"),
    "github_token": re.compile(r"gh[pousr]_[A-Za-z0-9]{36,}"),
    "google_api_key": re.compile(r"AIza[0-9A-Za-z_-]{35}"),
    "private_key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----"),
    "credential_uri": re.compile(r"(?:https?|postgres(?:ql)?|s3)://[^\s/:]+:([^\s/@]+)@"),
}
ML_PATTERNS = {
    "personal_account_path": re.compile(r"(?i)(?:[a-z]:[\\/]Users[\\/]|/Users/|/home/(?!iceberg(?:/|\b)))"),
    "credential_query": re.compile(r"(?i)[?&](?:access_token|token|password|secret|signature)=[^\s&]+"),
}
MAX_BYTES = 2 * 1024 * 1024


def triage(categories, data, allowlist):
    checksum = hashlib.sha256(data.replace(b"\r\n", b"\n")).hexdigest()
    approved = {item["category"] for item in allowlist if item["normalized_sha256"] == checksum}
    # Only exact, reviewed synthetic URI fixtures can be exempted; never keys or token patterns.
    known = [category for category in categories if category == "credential_uri" and category in approved]
    return [category for category in categories if category not in known], known


def scan_text(text, ml=False):
    categories = []
    for category, pattern in {**PATTERNS, **(ML_PATTERNS if ml else {})}.items():
        for match in pattern.finditer(text):
            # Shell interpolation, documented placeholders and fixture URI examples are not credentials.
            if category == "credential_uri" and any(marker in match.group(0).lower()
                    for marker in ("${", "change-me-", "<", "example", "fixture")):
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


def audit(root, include_history=True):
    root = root.resolve()
    report = {"schema_version": "1.0", "coverage": {}, "findings": [], "triaged_synthetic_fixtures": [], "pending": [], "can_mark_done": False}
    allow_path = root / "docs/security/SEC-01-synthetic-uris.json"
    allowlist = json.loads(allow_path.read_text(encoding="utf-8")) if allow_path.is_file() else []
    def record(scope, location, categories, data=None):
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
        record("worktree", name, scan_text(data.decode("utf-8", errors="ignore"), ml="ml/results/" in name or "ml-export/" in name), data)
        current += 1
    report["coverage"]["worktree_files"] = current
    env = root / ".env"
    if git(root, "ls-files", ".env").strip():
        record("config", ".env", ["tracked_local_env"])
    ignored = subprocess.run(["git", "-C", str(root), "check-ignore", "-q", ".env"], capture_output=True).returncode == 0
    if not ignored:
        record("config", ".env", ["local_env_not_ignored"])
    if env.exists():
        if env.is_symlink() or env.resolve().parent != root:
            record("config", ".env", ["unsafe_audit_path"])
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
    for directory in (root / "airflow/logs", root / "staging"):
        if not directory.exists():
            continue
        for path in directory.rglob("*.log"):
            if "m2" in path.relative_to(root).parts:
                continue
            name = path.relative_to(root).as_posix()
            if path.is_symlink() or root not in path.resolve().parents:
                record("logs", name, ["unsafe_audit_path"]); continue
            if path.stat().st_size > MAX_BYTES:
                report["pending"].append({"scope": "logs", "location": name, "reason": "file_size_guard"}); continue
            record("logs", name, scan_text(path.read_bytes().decode("utf-8", errors="ignore")))
            log_count += 1
    report["coverage"]["local_log_files"] = log_count
    command = ["docker", "compose", "-f", str(root / "compose.yaml"), "config", "--no-interpolate", "--no-env-resolution", "--format", "json"]
    try:
        result = subprocess.run(command, cwd=root, capture_output=True, timeout=30)
        if result.returncode:
            report["pending"].append({"scope": "compose", "reason": "unresolved_config_check_failed"})
        else:
            findings, ports = inspect_compose(json.loads(result.stdout))
            report["findings"].extend(findings); report["coverage"]["declared_ports"] = ports
    except (OSError, subprocess.TimeoutExpired, ValueError):
        report["pending"].append({"scope": "compose", "reason": "compose_cli_unavailable"})
    # No runtime operations or broad bucket enumeration. Future components cannot be certified.
    report["pending"].extend([
        {"scope": "runtime", "reason": "live_deployed_ports_logs_bucket_prefix_positive_negative_permission_tests_required"},
        {"scope": "ml", "reason": "EXP-01_MLD-05_MLI-03_actual_notebook_export_import_artifacts_not_available"},
    ])
    report["coverage"]["pattern_policy"] = "high_confidence_tokens_keys_and_credential_URIs_not_complete_secret_detection"
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    target = args.output.resolve()
    if root not in target.parents or args.output.is_symlink():
        parser.error("output must stay inside project and not be a symlink")
    try:
        report = audit(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(report, indent=2, ensure_ascii=False)+"\n", encoding="utf-8")
    except Exception:
        print("SEC-01 audit failed; diagnostic values redacted")
        return 1
    print("SEC-01 scoped audit:", len(report["findings"]), "findings;", len(report["pending"]), "pending coverage items; can_mark_done=false")
    return 1 if report["findings"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
