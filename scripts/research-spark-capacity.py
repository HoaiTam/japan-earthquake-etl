#!/usr/bin/env python3
"""DAT-03 read-only Spark count/unique/exact writer fingerprint stress. No lake/service/lease mutations."""
import argparse
import hashlib
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

CONTAINER="japan-earthquake-etl-airflow-scheduler-1"
CLASS="ie212.earthquake.spark.gold.ResearchGoldCapacityJob"


def require(ok, reason):
    if not ok: raise RuntimeError(reason)


def safe(path):
    p=Path(path).absolute()
    require(not any(q.is_symlink() for q in (p,*p.parents)) and ".." not in p.parts,"SPARK_CAPACITY_UNSAFE_PATH")
    return p


def sha(path):
    with safe(path).open("rb") as stream:return hashlib.file_digest(stream,"sha256").hexdigest()


def load(path):
    path=safe(path);require(path.is_file() and path.stat().st_size<=65536,"SPARK_CAPACITY_METADATA_BOUND")
    def unique(items):
        result={}
        for k,v in items:require(k not in result,"SPARK_CAPACITY_DUPLICATE_KEY");result[k]=v
        return result
    return json.loads(path.read_bytes(),object_pairs_hook=unique,parse_constant=lambda _:require(False,"SPARK_CAPACITY_NONFINITE"))


def save(path,value):
    with safe(path).open("x",encoding="utf-8") as stream:
        json.dump(value,stream,sort_keys=True,ensure_ascii=False,allow_nan=False);stream.write("\n")


def module(name):
    p=Path(__file__).resolve().parent/(name+".py");s=importlib.util.spec_from_file_location(name.replace("-","_"),p)
    m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m


def bounded_child(command,out,err,seconds):
    child=subprocess.Popen(command,stdout=out,stderr=err,start_new_session=True)
    try:return child.wait(timeout=seconds),False
    except subprocess.TimeoutExpired:
        os.killpg(child.pid,signal.SIGTERM)
        try:return child.wait(timeout=20),True
        except subprocess.TimeoutExpired:
            os.killpg(child.pid,signal.SIGKILL);return child.wait(timeout=10),True


def worker(root,seconds):
    import resource
    root=safe(root);require(re.fullmatch(r"/opt/pipeline/staging/dat03-spark-capacity/[a-f0-9]{32}",str(root)),"SPARK_CAPACITY_EXACT_ROOT")
    request=load(root/"request.json");require(sha(root/"runner.jar")==request["runner_sha256"],"SPARK_CAPACITY_RUNNER_CHANGED")
    require(not (Path(os.environ["SOURCE_GUARD_ROOT"])/"owner.json").exists(),"SPARK_CAPACITY_LEASE_NOT_QUIET")
    deps=Path("/opt/spark/jobs/iceberg");jars=[deps/n for n in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar","iceberg-aws-bundle-1.10.1.jar")]
    require(all(p.is_file() for p in jars) and 300<=seconds<=3600,"SPARK_CAPACITY_RUNTIME_NOT_PREPARED")
    cmd=["/opt/spark/bin/spark-submit","--master","local[1]","--driver-memory","768m","--conf","spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m",
         "--jars",",".join(map(str,jars)),"--class",CLASS,str(root/"runner.jar"),str(root/"request.json")]
    started=time.monotonic();timed_out=False
    with (root/"stdout.private").open("xb") as out,(root/"stderr.private").open("xb") as err:
        os.chmod(out.name,0o600);os.chmod(err.name,0o600);code,timed_out=bounded_child(cmd,out,err,seconds)
    save(root/"execution.json",{"exit_code":code,"timed_out":timed_out,"wall_seconds":round(time.monotonic()-started,3),
                                "child_peak_rss_kib":resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,"lease_mutations":False,"data_mutations":False})
    require(code==0 and not timed_out,"SPARK_CAPACITY_FAILED_NO_LEASE_MUTATIONS")
    require(load(root/"result.json")["status"]=="SPARK_FINGERPRINT_STRESS_PASSED","SPARK_CAPACITY_RESULT_REQUIRED")
    print("event=dat03_spark_capacity_complete",flush=True)


TRANSFER=r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-spark-capacity/[a-f0-9]{32}",str(p))
assert not any(q.is_symlink() for q in (p,*p.parents))
files=json.loads(sys.stdin.buffer.readline());assert {f["name"] for f in files}=={"runner.jar","request.json","controller.py"}
assert len(files)==3 and sum(f["bytes"] for f in files)<=64*1024**2
p.parent.mkdir(exist_ok=True);p.mkdir(mode=0o700)
for f in files:
 assert type(f["bytes"]) is int and 0<f["bytes"]<=64*1024**2
 left=f["bytes"];d=hashlib.sha256()
 with (p/f["name"]).open("xb") as out:
  os.chmod(out.name,0o600)
  while left:
   b=sys.stdin.buffer.read(min(left,1024**2));assert b;left-=len(b);d.update(b);out.write(b)
 assert d.hexdigest()==f["sha256"]
assert not sys.stdin.buffer.read(1)
'''


def run(deployment,catalog,table,copies,partitions,seconds):
    require(table in ("event_current","event_source_bridge") and type(copies) is int and 1<=copies<=64
            and type(partitions) is int and partitions in (1,4,16) and type(seconds) is int and 300<=seconds<=3600,"SPARK_CAPACITY_TYPED_LIMITS_REQUIRED")
    deployment,catalog=map(safe,(deployment,catalog));c=module("research-catalog");r=module("research-rollout")
    c.validate(catalog);plan=c.load(catalog/"plan.json");summary=c.load(catalog/"usgs-summary.json")
    required=sum(s["profile_accepted_rows"] for s in plan["splits"].values())+summary["sum_response_features_including_halo_duplicates"]
    bounded=c.load(catalog/"bounded-report.json");source=bounded["cases"][2]["first"]["commit"]["tables"][table]
    require(source["count"]*copies>=required,"SPARK_CAPACITY_WORKLOAD_INSUFFICIENT")
    require(shutil.disk_usage(deployment).free>=4*1024**3,"SPARK_CAPACITY_DISK_RESERVE")
    before=r.services();r.idle();r.trino_queries_idle()
    jar=safe(Path(__file__).resolve().parents[1]/"spark/target/japan-earthquake-etl-runner.jar")
    import zipfile
    entry="ie212/earthquake/spark/gold/GoldIcebergWriter.class"
    with zipfile.ZipFile(jar) as z,zipfile.ZipFile(catalog/"runner.jar") as original:
        require(CLASS.replace(".","/")+".class" in z.namelist(),"SPARK_CAPACITY_BUILD_LOCAL_JAR_FIRST")
        require(z.getinfo(entry).file_size<=1024**2 and original.getinfo(entry).file_size<=1024**2
                and z.read(entry)==original.read(entry),"SPARK_CAPACITY_EXACT_ORIGINAL_WRITER_REQUIRED")
        writer_sha=hashlib.sha256(z.read(entry)).hexdigest()
    root=deployment/"staging/dat03-spark-capacity"/uuid.uuid4().hex;root.mkdir(parents=True,mode=0o700)
    for path,name in ((jar,"runner.jar"),(Path(__file__),"controller.py")):
        with path.open("rb") as src,(root/name).open("xb") as dest:os.chmod(dest.name,0o600);shutil.copyfileobj(src,dest)
    request={"version":"dat03-spark-gold-capacity-v1","namespace":bounded["namespace"],"table":table,"snapshot_id":source["snapshot_id"],
             "source_count":source["count"],"copies":copies,"shuffle_partitions":partitions,"required_rows":required,"runner_sha256":sha(root/"runner.jar"),"writer_class_sha256":writer_sha}
    save(root/"request.json",request);inside="/opt/pipeline/staging/dat03-spark-capacity/"+root.name
    files=[{"name":n,"bytes":(root/n).stat().st_size,"sha256":sha(root/n)} for n in ("runner.jar","request.json","controller.py")]
    with subprocess.Popen(["docker","exec","-i",CONTAINER,"python3","-c",TRANSFER,inside],stdin=subprocess.PIPE) as child:
        child.stdin.write((json.dumps(files)+"\n").encode())
        for f in files:
            with (root/f["name"]).open("rb") as stream:shutil.copyfileobj(stream,child.stdin,1024**2)
        child.stdin.close();require(child.wait(timeout=120)==0,"SPARK_CAPACITY_STAGE_FAILED")
    bootstrap='import hashlib,runpy,sys;from pathlib import Path;p=Path(sys.argv[1]);assert hashlib.sha256((p/"controller.py").read_bytes()).hexdigest()==sys.argv[2];sys.argv=[str(p/"controller.py"),"worker","--folder",str(p),"--seconds",sys.argv[3]];runpy.run_path(str(p/"controller.py"),run_name="__main__")'
    try:completed=subprocess.run(["docker","exec",CONTAINER,"python3","-c",bootstrap,inside,sha(root/"controller.py"),str(seconds)],timeout=seconds+120,check=False)
    finally:subprocess.run(["docker","cp",CONTAINER+":"+inside,str(root/"runtime")],timeout=120,check=True)
    after=r.services();r.idle();r.trino_queries_idle()
    require(before==after,"SPARK_CAPACITY_SERVICES_CHANGED")
    save(root/"postflight.json",{"status":"QUIET_HEALTHY_UNCHANGED","worker_exit_code":completed.returncode,
                                "services_before":before,"services_after":after,"data_mutations":False,"lease_mutations":False})
    print("evidence_root="+str(root),flush=True)
    require(completed.returncode==0,"SPARK_CAPACITY_NOT_ACCEPTED")


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("mode",choices=("run","worker"));p.add_argument("--deployment-root",type=Path)
    p.add_argument("--catalog",type=Path);p.add_argument("--folder",type=Path);p.add_argument("--table",choices=("event_current","event_source_bridge"),default="event_current")
    p.add_argument("--copies",type=int,default=43);p.add_argument("--shuffle-partitions",type=int,choices=(1,4,16),default=1);p.add_argument("--seconds",type=int,default=1200)
    a=p.parse_args()
    if a.mode=="worker":worker(a.folder,a.seconds)
    else:
        p.error("exact deployment/catalog roots required") if a.deployment_root is None or a.catalog is None else None
        run(a.deployment_root,a.catalog,a.table,a.copies,a.shuffle_partitions,a.seconds)


if __name__=="__main__":main()
