"""Control-plane wrapper: bounded Java/Spark read-only pilot under the shared lease."""
import json
import os
from pathlib import Path
import subprocess
import pwd
import tempfile
import uuid
from urllib.request import urlopen

from backfill_runtime import resolve, require, BackfillError
from jma_backfill_runtime import _atomic_json
from source_run_guard import lease


def memory():
    root = Path("/sys/fs/cgroup")
    def count(name):
        path = root / name
        return int(path.read_text().strip()) if path.is_file() else None
    events = root / "memory.events"
    return {"peak_bytes": count("memory.peak"), "current_bytes": count("memory.current"),
            "events": dict((key, int(value)) for key, value in
                           (line.split() for line in events.read_text().splitlines())) if events.exists() else None}


def run(environment=None, conf=None):
    env = os.environ if environment is None else environment
    conf = conf or json.loads((Path(__file__).parent / "fixtures/orc_03_reuse_sample.json").read_text())
    plan = resolve({"dag_run": {"conf": conf}}, env)
    run_id = "orc05-resource-qa-" + uuid.uuid4().hex
    identity = {"dag_id": "orc_03_backfill", "run_id": run_id}
    root = Path(env.get("BACKFILL_STAGING_ROOT", "/opt/pipeline/staging/backfill")) / "qa" / run_id
    request = root / "resource-input.json"
    # Acquire BEFORE writing any QA staging metadata / touching exact inputs.
    lease("acquire", identity, env)
    try:
        before = memory()
        with urlopen("http://spark-master:8080/json/", timeout=10) as response:
            status = json.loads(response.read(65537))
        require(not status.get("activeapps") and any(worker.get("state") == "ALIVE"
                    and worker.get("coresfree", 0) >= 1 and worker.get("memoryfree", 0) >= 512
                    for worker in status.get("workers", [])), "RESOURCE_PILOT_CLUSTER_BUSY")
        _atomic_json(request, {"contract_version": "orc-03-v1", "scope_sha256": plan["scope_sha256"],
                               "bronze_inputs": plan["bronze_inputs"], "observability_version": "orc-04-v1"})
        command = ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077",
            "--deploy-mode", "client", "--class", "ie212.earthquake.spark.ResourcePilotJob",
            "--driver-memory", "512m", "--executor-memory", "512m", "--executor-cores", "1",
            "--conf", "spark.driver.host=spark-resource-pilot", "--conf", "spark.driver.bindAddress=0.0.0.0",
            "--conf", "spark.cores.max=1", "--conf", "spark.dynamicAllocation.enabled=false",
            "--conf", "spark.sql.shuffle.partitions=4", "--conf", "spark.default.parallelism=1",
            "--conf", "spark.driver.maxResultSize=64m", "--conf", "spark.ui.enabled=false",
            "/opt/spark/jobs/japan-earthquake-etl-runner.jar", str(request)]
        # Spool stdout to disk, then cap accepted metadata at 64KiB; never echo raw errors.
        with tempfile.TemporaryDirectory(prefix="orc05-identity-") as identity_root, (root / "result.tmp").open("w+b") as spool:
            child_env = java_identity(env, Path(identity_root))
            result = subprocess.run(command, env=child_env, stdout=spool, stderr=subprocess.DEVNULL, timeout=300, check=False)
            require(result.returncode == 0 and spool.tell() <= 65536, "RESOURCE_PILOT_FAILED")
            spool.seek(0); report = json.loads(spool.read())
        require(report["task"] == "ORC-05" and report["lake_writes"] is False
                and report["published"] is False and report["master"] == "spark://spark-master:7077",
                "RESOURCE_PILOT_FAILED")
        after = memory()
        require(after["events"] is not None and before["events"] is not None
                and after["events"]["oom_kill"] == before["events"]["oom_kill"], "RESOURCE_PILOT_OOM")
        report.update(run_id=run_id, scope_sha256=plan["scope_sha256"], driver_cgroup_before=before,
                      driver_cgroup_after=after, report_path=str(root / "resource_report.json"))
        _atomic_json(root / "resource_report.json", report)
        return report
    finally:
        released = lease("release", identity, env)
        require(released["status"] == "RELEASED", "RESOURCE_PILOT_LEASE_NOT_RELEASED")


def java_identity(environment, root):
    """Spark/Hadoop Unix login needs a passwd entry for the shared Airflow UID.

    Use the image's NSS wrapper, not root/chown or a permanent /etc/passwd edit.
    Temporary files contain only UID/GID, no credential, and live through submit.
    """
    env = dict(environment)
    try:
        pwd.getpwuid(os.getuid())
        return env
    except KeyError:
        pass
    candidates = [Path(prefix) / suffix / "libnss_wrapper.so" for prefix in ("/usr/lib", "/lib")
                  for suffix in ("aarch64-linux-gnu", "x86_64-linux-gnu", "")]
    wrapper = next((path for path in candidates if path.is_file()), None)
    require(wrapper is not None, "RESOURCE_PILOT_UID_UNRESOLVED")
    passwd, group = root / "passwd", root / "group"
    passwd.write_text(f"airflow:x:{os.getuid()}:{os.getgid()}:pilot:/tmp:/bin/false\n")
    group.write_text(f"airflow:x:{os.getgid()}:\n")
    env.update(LD_PRELOAD=str(wrapper), NSS_WRAPPER_PASSWD=str(passwd), NSS_WRAPPER_GROUP=str(group))
    return env


if __name__ == "__main__":
    try:
        print(json.dumps(run(), sort_keys=True))
    except Exception as error:
        safe = {"RESOURCE_PILOT_FAILED", "RESOURCE_PILOT_OOM", "RESOURCE_PILOT_CLUSTER_BUSY",
                "RESOURCE_PILOT_UID_UNRESOLVED", "RESOURCE_PILOT_LEASE_NOT_RELEASED"}
        reason = str(error) if isinstance(error, BackfillError) and str(error) in safe else "ORC05_RESOURCE_PILOT_FAILED"
        raise SystemExit(reason) from None
