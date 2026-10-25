#!/usr/bin/env python3
"""Explicit operator-approved Trino-only profile trial. Never build/pull/init/alter data or credentials."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dat03_rollout", ROOT / "scripts/research-rollout.py")
r = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(r)
TRINO = "japan-earthquake-etl-trino-1"
CONFIGS = {"config.properties": "/etc/trino/config.properties", "jvm.config": "/etc/trino/jvm.config"}


def command(args, timeout=30):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout).stdout


def inspect():
    names = ["japan-earthquake-etl-" + s + "-1" for s in r.SERVICES]
    return {row["Name"].lstrip("/"): row for row in json.loads(command(["docker", "inspect", *names]))}


def env_pin(values):
    return hashlib.sha256(json.dumps(sorted(values), separators=(",", ":")).encode()).hexdigest()


def snapshot(rows):
    # Deliberately exclude raw environment, command line, labels and credentials.
    return {name: {"id": row["Id"], "image": row["Image"], "started_at": row["State"]["StartedAt"], "restarts": row["RestartCount"],
                   "environment_sha256": env_pin(row["Config"].get("Env", [])), "user": row["Config"]["User"],
                   "memory": row["HostConfig"]["Memory"], "nano_cpus": row["HostConfig"]["NanoCpus"],
                   "security": {k: row["HostConfig"].get(k) for k in ("CapDrop", "CapAdd", "SecurityOpt", "ReadonlyRootfs", "PortBindings", "NetworkMode")}}
            for name, row in rows.items()}


def config_files(row, deployment):
    labels = row["Config"]["Labels"]
    r.require(labels.get("com.docker.compose.project") == "japan-earthquake-etl"
              and r.safe(labels["com.docker.compose.project.working_dir"]) == deployment, "TRINO_PROFILE_PROJECT_MISMATCH")
    files = [r.safe(Path(p)) for p in labels["com.docker.compose.project.config_files"].split(",")]
    r.require(1 <= len(files) <= 8 and len(set(files)) == len(files)
              and files[0] == deployment / "compose.yaml" and all(p.is_relative_to(deployment) and p.is_file() for p in files), "TRINO_PROFILE_EXACT_COMPOSE_FILES_REQUIRED")
    return files


def same_environment_and_image(compiled, old, image_environment=None):
    old_env = dict(v.split("=", 1) for v in old["Config"].get("Env", []))
    if image_environment is None:
        image_environment=json.loads(command(["docker","image","inspect",old["Image"]]))[0]["Config"].get("Env",[])
    expected=dict(v.split("=",1) for v in image_environment)
    expected.update({k:str(v) for k,v in compiled.get("environment",{}).items()})
    r.require(expected == old_env and compiled["image"] == old["Config"]["Image"], "TRINO_PROFILE_IMAGE_OR_ENVIRONMENT_DRIFT")


def immutable_private(path, data):
    with r.safe(path).open("xb") as out:
        os.chmod(path, 0o600); out.write(data)


def verify_backup_and_candidates(folder, plan):
    """Recheck exact profile set and retained backups before any runtime access."""
    r.require(set(plan["profiles"]) == set(CONFIGS), "TRINO_PROFILE_EXACT_CONFIG_SET_REQUIRED")
    for name, pin in plan["profiles"].items():
        candidate = r.safe(pin["candidate"])
        r.require(candidate == ROOT / "trino/research" / name
                  and r.profile.sha(candidate) == pin["candidate_sha256"], "TRINO_PROFILE_CANDIDATE_CHANGED")
    for name, pin in plan["backup_sha256"].items():
        path = r.safe(folder / name)
        r.require(path.is_relative_to(folder) and r.profile.sha(path) == pin, "TRINO_PROFILE_BACKUP_CHANGED")


def verify_compose_pins(plan):
    pins = plan.get("compose_sha256", {})
    r.require(set(pins) == set(plan["config_files"]) and pins, "TRINO_PROFILE_COMPOSE_CONTENT_PINS_REQUIRED")
    for name, pin in pins.items():
        path = r.safe(name)
        r.require(path.is_relative_to(r.safe(plan["deployment"])) and r.profile.sha(path) == pin, "TRINO_PROFILE_COMPOSE_CONTENT_CHANGED")


def queries_idle():
    # Refuse to recreate while ANY query is active, not just this probe's user.
    script = '''import os,json,urllib.request
u="http://"+os.environ["TRINO_HOST"]+":"+os.environ["TRINO_INTERNAL_PORT"]+"/v1/query"
with urllib.request.urlopen(urllib.request.Request(u,headers={"X-Trino-User":"dat03-capacity-probe"}),timeout=15) as reply:
    raw=reply.read(4*1024*1024+1)
assert len(raw)<=4*1024*1024
rows=json.loads(raw)
assert isinstance(rows,list) and all(x.get("state") in ("FINISHED","FAILED") for x in rows), "TRINO_PROFILE_ACTIVE_QUERY"
print("TRINO_QUERIES_IDLE")
'''
    r.require(command(["docker", "exec", "japan-earthquake-etl-airflow-scheduler-1", "python3", "-c", script]).strip() == "TRINO_QUERIES_IDLE", "TRINO_PROFILE_QUERY_PREFLIGHT_FAILED")


def prepare(deployment):
    deployment = r.safe(deployment); r.services(); r.idle(); rows = inspect(); old = rows[TRINO]
    r.require(old["State"]["Status"] == "running" and old["State"].get("Health", {}).get("Status") == "healthy", "TRINO_PROFILE_HEALTH_REQUIRED")
    queries_idle()
    files = config_files(old, deployment)
    folder = r.safe(deployment / "staging" / ("dat03-trino-research-" + uuid.uuid4().hex)); folder.mkdir(mode=0o700)
    mounts = {m["Destination"]: m for m in old["Mounts"]}; pins = {}
    for name, destination in CONFIGS.items():
        mount = mounts[destination]; original = r.safe(mount["Source"]); candidate = r.safe(ROOT / "trino/research" / name)
        r.require(mount["Type"] == "bind" and mount["RW"] is False and original.is_file() and original.stat().st_size <= 65536
                  and candidate.is_file() and candidate.stat().st_size <= 65536, "TRINO_PROFILE_READONLY_CONFIG_REQUIRED")
        immutable_private(folder / ("before-" + name), original.read_bytes())
        pins[name] = {"before_sha256": r.profile.sha(original), "candidate_sha256": r.profile.sha(candidate), "candidate": str(candidate)}
    # Private retained logs before removing the old writable container layer.
    logs = subprocess.run(["docker", "logs", "--timestamps", TRINO], check=True, capture_output=True, timeout=30)
    r.require(len(logs.stdout) + len(logs.stderr) <= 32 * 1024**2, "TRINO_PROFILE_LOG_BACKUP_BOUND")
    immutable_private(folder / "container.stdout.private", logs.stdout); immutable_private(folder / "container.stderr.private", logs.stderr)
    command(["docker", "cp", TRINO + ":/data/trino/var/log", str(folder / "retained-logs")], timeout=60)
    for p in (folder / "retained-logs").rglob("*"):
        r.require(not p.is_symlink(), "TRINO_PROFILE_LOG_SYMLINK_REJECTED")
        if p.is_file(): os.chmod(p, 0o600)
    override = "services:\n  trino:\n    volumes:\n"
    for name, destination in CONFIGS.items():
        override += "      - type: bind\n        source: " + json.dumps(str(ROOT / "trino/research" / name)) + "\n        target: " + destination + "\n        read_only: true\n"
    immutable_private(folder / "compose.override.yaml", override.encode())
    base = ["docker", "compose", "--project-directory", str(deployment), "-p", "japan-earthquake-etl"]
    for file in [*files, folder / "compose.override.yaml"]: base.extend(["-f", str(file)])
    # Parse in memory only: compose config can contain environment credentials.
    compiled = json.loads(command([*base, "config", "--format", "json"]))["services"]["trino"]
    same_environment_and_image(compiled,old)
    logs_index = {str(p.relative_to(folder)): r.profile.sha(p) for p in folder.rglob("*") if p.is_file()}
    r.save(folder / "plan.json", {"version": "dat03-trino-research-profile-v1", "deployment": str(deployment), "config_files": [str(p) for p in files],
                                  "compose_sha256": {str(p): r.profile.sha(p) for p in files},
                                  "before": snapshot(rows), "profiles": pins, "backup_sha256": logs_index,
                                  "operator_approval_required": True, "applied": False, "data_mutations": False})
    print("event=dat03_trino_profile_prepared folder=" + str(folder), flush=True)
    return folder


def apply(folder):
    folder = r.safe(folder); plan = r.load(folder / "plan.json"); deployment = r.safe(plan["deployment"])
    r.require(folder.parent == deployment / "staging" and re.fullmatch(r"dat03-trino-research-[a-f0-9]{32}", folder.name)
              and plan["version"] == "dat03-trino-research-profile-v1"
              and not any((folder / name).exists() for name in ("applied.json","execution.json","apply.stdout.private","apply.stderr.private")),
              "TRINO_PROFILE_EXACT_NEW_ATTEMPT_REQUIRED")
    verify_backup_and_candidates(folder, plan)
    verify_compose_pins(plan)
    r.services(); r.idle(); queries_idle(); before = inspect()
    r.require(snapshot(before) == plan["before"], "TRINO_PROFILE_RUNTIME_CHANGED")
    r.require(set(plan["profiles"]) == set(CONFIGS), "TRINO_PROFILE_EXACT_CONFIG_SET_REQUIRED")
    for name, pin in plan["profiles"].items():
        r.require(r.safe(pin["candidate"]) == ROOT / "trino/research" / name and r.profile.sha(r.safe(pin["candidate"])) == pin["candidate_sha256"], "TRINO_PROFILE_CANDIDATE_CHANGED")
        original = next(m for m in before[TRINO]["Mounts"] if m["Destination"] == CONFIGS[name])
        r.require(r.profile.sha(r.safe(original["Source"])) == pin["before_sha256"], "TRINO_PROFILE_ORIGINAL_CONFIG_CHANGED")
    base = ["docker", "compose", "--project-directory", str(deployment), "-p", "japan-earthquake-etl"]
    for file in [*config_files(before[TRINO], deployment), folder / "compose.override.yaml"]: base.extend(["-f", str(file)])
    same_environment_and_image(json.loads(command([*base,"config","--format","json"]))["services"]["trino"],before[TRINO])
    started = time.monotonic()
    try:
        result = subprocess.run([*base, "up", "-d", "--no-deps", "--no-build", "--pull", "never", "trino"], check=False, capture_output=True, timeout=90)
    except subprocess.TimeoutExpired as error:
        immutable_private(folder / "apply.stdout.private", error.stdout or b"")
        immutable_private(folder / "apply.stderr.private", error.stderr or b"")
        r.save(folder / "execution.json", {"status":"APPLY_OUTCOME_UNKNOWN_REQUIRES_INSPECT","retry":False})
        raise ValueError("TRINO_PROFILE_APPLY_OUTCOME_UNKNOWN_NO_RETRY") from None
    immutable_private(folder / "apply.stdout.private", result.stdout); immutable_private(folder / "apply.stderr.private", result.stderr)
    r.save(folder / "execution.json", {"status":"COMPOSE_RETURNED","exit_code":result.returncode,"retry":False})
    r.require(result.returncode == 0, "TRINO_PROFILE_RECREATE_FAILED_REVIEW_PRIVATE_LOG")
    after = None
    while time.monotonic() - started < 180:
        after = inspect()
        if after[TRINO]["State"].get("Health", {}).get("Status") == "healthy": break
        time.sleep(2)
    r.require(after is not None and after[TRINO]["State"].get("Health", {}).get("Status") == "healthy", "TRINO_PROFILE_NOT_HEALTHY")
    old_summary, new_summary = plan["before"], snapshot(after)
    for name in old_summary:
        if name != TRINO: r.require(old_summary[name] == new_summary[name], "TRINO_PROFILE_OTHER_SERVICE_CHANGED")
    for key in ("image", "environment_sha256", "user", "memory", "nano_cpus", "security"):
        r.require(old_summary[TRINO][key] == new_summary[TRINO][key], "TRINO_PROFILE_UNEXPECTED_RUNTIME_DRIFT")
    old_mounts = {m["Destination"]: {k: m[k] for k in ("Source", "Type", "RW")} for m in before[TRINO]["Mounts"]}
    new_mounts = {m["Destination"]: {k: m[k] for k in ("Source", "Type", "RW")} for m in after[TRINO]["Mounts"]}
    for name, destination in CONFIGS.items(): old_mounts[destination]["Source"] = str(ROOT / "trino/research" / name)
    r.require(old_mounts == new_mounts, "TRINO_PROFILE_UNEXPECTED_MOUNT_DRIFT")
    r.require(old_summary[TRINO]["id"] != new_summary[TRINO]["id"], "TRINO_PROFILE_CONTAINER_NOT_RECREATED")
    r.idle()
    receipt = {"version": plan["version"], "status": "PROFILE_DEPLOYED_NOT_CAPACITY_ACCEPTED", "plan_sha256": r.profile.sha(folder / "plan.json"),
               "controller_sha256": r.profile.sha(Path(__file__)),
               "after": new_summary, "wall_seconds": round(time.monotonic() - started, 3), "build_pull_or_init": False,
               "only_trino_recreated": True, "credentials_unchanged": True, "data_mutations": False, "full_capacity_accepted": False}
    r.save(folder / "applied.json", receipt)
    print("event=dat03_trino_profile_deployed capacity_accepted=false folder=" + str(folder), flush=True)


def verify(folder):
    """Read-only late-health reconciliation. Never runs compose up a second time."""
    folder=r.safe(folder);plan=r.load(folder/"plan.json");deployment=r.safe(plan["deployment"])
    r.require(folder.parent==deployment/"staging" and re.fullmatch(r"dat03-trino-research-[a-f0-9]{32}",folder.name)
              and plan["version"]=="dat03-trino-research-profile-v1","TRINO_PROFILE_EXACT_NEW_ATTEMPT_REQUIRED")
    execution=r.load(folder/"execution.json")
    r.require(execution.get("status")=="COMPOSE_RETURNED" and execution.get("exit_code")==0,"TRINO_PROFILE_RECONCILE_EXECUTION_REQUIRED")
    verify_backup_and_candidates(folder, plan)
    if "compose_sha256" in plan: verify_compose_pins(plan)
    r.services();r.idle();queries_idle();rows=inspect();after=snapshot(rows);before=plan["before"]
    for name in before:
        if name!=TRINO:r.require(before[name]==after[name],"TRINO_PROFILE_OTHER_SERVICE_CHANGED")
    for key in ("image","environment_sha256","user","memory","nano_cpus","security"):
        r.require(before[TRINO][key]==after[TRINO][key],"TRINO_PROFILE_UNEXPECTED_RUNTIME_DRIFT")
    r.require(before[TRINO]["id"]!=after[TRINO]["id"],"TRINO_PROFILE_CONTAINER_NOT_RECREATED")
    files=config_files(rows[TRINO],deployment)
    expected=[r.safe(f) for f in plan["config_files"]]+[folder/"compose.override.yaml"]
    r.require(files==expected,"TRINO_PROFILE_COMPOSE_CHANGED")
    base=["docker","compose","--project-directory",str(deployment),"-p","japan-earthquake-etl"]
    for file in files:base.extend(["-f",str(file)])
    compiled=json.loads(command([*base,"config","--format","json"]))["services"]["trino"]
    same_environment_and_image(compiled,rows[TRINO])
    actual={m["Destination"]:{"Source":m["Source"],"Type":m["Type"],"RW":m["RW"]} for m in rows[TRINO]["Mounts"]}
    desired={m["target"]:{"Source":str(r.safe(m["source"])),"Type":m["type"],"RW":not m.get("read_only",False)} for m in compiled["volumes"]}
    r.require(actual==desired,"TRINO_PROFILE_UNEXPECTED_MOUNT_DRIFT")
    for name,pin in plan["profiles"].items():
        candidate=r.safe(pin["candidate"])
        r.require(candidate==ROOT/"trino/research"/name and r.profile.sha(candidate)==pin["candidate_sha256"]
                  and actual[CONFIGS[name]]=={"Source":str(candidate),"Type":"bind","RW":False},"TRINO_PROFILE_CANDIDATE_CHANGED")
    receipt={"version":plan["version"],"status":"LATE_HEALTH_VERIFIED_NOT_CAPACITY_ACCEPTED","plan_sha256":r.profile.sha(folder/"plan.json"),
             "execution_sha256":r.profile.sha(folder/"execution.json"),"controller_sha256":r.profile.sha(Path(__file__)),"after":after,
             "initial_180_second_health_acceptance_passed":False,"only_trino_recreated":True,"credentials_unchanged":True,
             "compose_source_content_pins_present":"compose_sha256" in plan,
             "mounts_verified_against_compiled_pinned_compose":True,"data_mutations":False,"full_capacity_accepted":False}
    r.save(folder/"late-health.json",receipt)
    print("event=dat03_trino_late_health_verified capacity_accepted=false folder="+str(folder),flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("mode", choices=("prepare", "apply", "verify"))
    parser.add_argument("--deployment-root", type=Path); parser.add_argument("--folder", type=Path)
    parser.add_argument("--operator-approved-trino-only", action="store_true"); args = parser.parse_args()
    if args.mode == "prepare":
        r.require(args.deployment_root is not None, "TRINO_PROFILE_DEPLOYMENT_REQUIRED"); prepare(args.deployment_root)
    elif args.mode == "verify":
        r.require(args.folder is not None,"TRINO_PROFILE_EXACT_FOLDER_REQUIRED");verify(args.folder)
    else:
        r.require(args.operator_approved_trino_only and args.folder is not None, "TRINO_PROFILE_OPERATOR_APPROVAL_REQUIRED"); apply(args.folder)


if __name__ == "__main__": main()
