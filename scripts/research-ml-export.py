#!/usr/bin/env python3
"""DAT-03 exact full-catalog MLI-02 execution, checkpointed <=2h sessions. No model fit/cloud/service mutations."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
import zipfile

HERE=Path(__file__).resolve().parent
CLASS="ie212.earthquake.spark.usgs.ResearchDatasetBuildJob"
VERSION="dat03-research-dataset-build-v1"
FILES=("runner.jar","request.json","feature-scaling.json","controller.py","catalog-control.py","research-e2e.py","child-control.py","catalog-acceptance-pin.json")
MIN_SECONDS=4200
MAX_SECONDS=6600
JAVA_REPORT=re.compile(r"sessions/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\.json")


def module(name,path):
    s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m


def support():
    return module("dataset_catalog",HERE/("catalog-control.py" if (HERE/"catalog-control.py").is_file() else "research-catalog.py"))


def full_catalog(folder,session):
    c=support();request=c.validate(folder);session=c.e.safe(session)
    c.e.require(session.parent==folder/"sessions" and re.fullmatch(r"[a-f0-9]{32}",session.name),"RESEARCH_EXPORT_EXACT_CATALOG_SESSION_REQUIRED")
    pin=c.load(session/"acceptance.json",65536);measurement=c.load(session/"execution.json",65536);result=c.session_result(folder,request,session)
    c.e.require(pin["status"]=="CHECKPOINT_ACCEPTED" and pin["request_sha256"]==c.e.sha(folder/"request.json")
                and pin["result_sha256"]==c.e.sha(session/"result.json") and pin["execution_sha256"]==c.e.sha(session/"execution.json")
                and type(measurement["exit_code"]) is int and measurement["exit_code"]==0 and measurement["timed_out"] is False
                and type(result["completed_months"]) is int and result["completed_months"]==288
                and result["full_research_catalog"] is True and result["pending_month"] is None
                and pin["lease_release"]=={"status":"RELEASED","dag_id":c.DAG,"run_id":request["run_id"]},"RESEARCH_EXPORT_FULL_288_MONTH_ACCEPTANCE_REQUIRED")
    return result


def prepare(deployment,catalog,observation,session,split):
    c=support();deployment,catalog,observation,session=map(c.e.safe,(deployment,catalog,observation,session))
    c.e.require(split in ("REPRODUCTION","EXTENSION"),"RESEARCH_EXPORT_SPLIT_REQUIRED")
    relative=catalog.relative_to(deployment/"staging/dat03-catalog")
    c.e.require(re.fullmatch(r"[a-f0-9]{32}",str(relative)) and observation.is_relative_to(catalog)
                and observation!=catalog,"RESEARCH_EXPORT_EXACT_CATALOG_COPY_REQUIRED")
    c.validate(catalog);c.e.require(c.e.sha(catalog/"request.json")==c.e.sha(observation/"request.json"),"RESEARCH_EXPORT_CATALOG_COPY_CHANGED")
    full_catalog(observation,session)  # No directory, lease or Docker action before the full-source gate.
    c.e.require(shutil.disk_usage(deployment).free>=4*1024**3,"RESEARCH_EXPORT_DISK_RESERVE")
    jar=c.e.safe(HERE.parent/"spark/target/japan-earthquake-etl-runner.jar")
    with zipfile.ZipFile(jar) as z:c.e.require(CLASS.replace(".","/")+".class" in z.namelist(),"RESEARCH_EXPORT_BUILD_LOCAL_ML_JAR_FIRST")
    root=deployment/"staging/dat03-ml-export"/uuid.uuid4().hex;root.mkdir(parents=True,mode=0o700)
    sources=((jar,"runner.jar"),(Path(__file__),"controller.py"),(HERE/"research-catalog.py","catalog-control.py"),
             (HERE/"research-e2e.py","research-e2e.py"),(HERE/"research-spark-capacity.py","child-control.py"),
             (HERE.parent/"config/ml/feature_scaling_global_dev_v1.json","feature-scaling.json"))
    for source,name in sources:
        before=c.e.sha(source)
        with source.open("rb") as src,(root/name).open("xb") as target:os.chmod(target.name,0o600);shutil.copyfileobj(src,target,1024**2)
        c.e.require(before==c.e.sha(source)==c.e.sha(root/name),"RESEARCH_EXPORT_SOURCE_CHANGED_DURING_COPY")
    inside="/opt/pipeline/staging/dat03-catalog/"+catalog.name
    request={"version":VERSION,"catalog_root":inside,"catalog_request_sha256":c.e.sha(catalog/"request.json"),"runner_sha256":c.e.sha(root/"runner.jar"),
             "scaling_sha256":c.e.sha(root/"feature-scaling.json"),"dataset_split":split,
             "observation_cutoff_utc":datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00","Z")}
    c.e.save(root/"request.json",request)
    c.e.save(root/"catalog-acceptance-pin.json",{"catalog_request_sha256":request["catalog_request_sha256"],"session":session.name,
                                               "acceptance_sha256":c.e.sha(session/"acceptance.json")})
    c.e.save(root/"inputs.json",{name:{"bytes":(root/name).stat().st_size,"sha256":c.e.sha(root/name)} for name in FILES})
    return root


def worker(root,seconds):
    import resource
    c=support();root=c.e.safe(root)
    c.e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-ml-export/[a-f0-9]{32}",str(root))
                and type(seconds) is int and MIN_SECONDS<=seconds<=MAX_SECONDS,"RESEARCH_EXPORT_EXACT_SESSION_REQUIRED")
    r=c.load(root/"request.json",65536)
    c.e.require(set(r)=={"version","catalog_root","catalog_request_sha256","runner_sha256","scaling_sha256","dataset_split","observation_cutoff_utc"}
                and r["version"]==VERSION and r["dataset_split"] in ("REPRODUCTION","EXTENSION")
                and re.fullmatch(r"/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}",r["catalog_root"])
                and all(re.fullmatch(r"[a-f0-9]{64}",r[k]) for k in ("catalog_request_sha256","runner_sha256","scaling_sha256"))
                and c.e.sha(root/"runner.jar")==r["runner_sha256"] and c.e.sha(root/"feature-scaling.json")==r["scaling_sha256"],
                "RESEARCH_EXPORT_CODE_CHANGED")
    catalog=Path(r["catalog_root"]);c.validate(catalog)
    c.e.require(c.e.sha(catalog/"request.json")==r["catalog_request_sha256"],"RESEARCH_EXPORT_CATALOG_CHANGED")
    source_pin=c.load(root/"catalog-acceptance-pin.json",65536)
    c.e.require(re.fullmatch(r"[a-f0-9]{32}",source_pin["session"]) and source_pin["catalog_request_sha256"]==r["catalog_request_sha256"],"RESEARCH_EXPORT_SOURCE_SESSION_CHANGED")
    source_session=catalog/"sessions"/source_pin["session"]
    c.e.require(c.e.sha(source_session/"acceptance.json")==source_pin["acceptance_sha256"],"RESEARCH_EXPORT_SOURCE_ACCEPTANCE_CHANGED")
    full_catalog(catalog,source_session)
    owner={"dag_id":"mli_02_build_dataset","run_id":"dat03-collection-"+root.name};guard=Path(os.environ["SOURCE_GUARD_ROOT"])/"owner.json"
    held=c.load(guard,65536) if guard.exists() else None
    if held is None:c.quiet()
    else:c.e.require(held==owner,"RESEARCH_EXPORT_FOREIGN_LEASE");c.quiet(owner)
    deps=Path("/opt/spark/jobs/iceberg");jars=[deps/n for n in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar","iceberg-aws-bundle-1.10.1.jar")]
    c.e.require(all(p.is_file() for p in jars) and shutil.disk_usage(root).free>=4*1024**3,"RESEARCH_EXPORT_DEPS_OR_DISK_RESERVE")
    previous=c.e.safe(root/"execution-sessions")
    old_sessions=list(previous.iterdir()) if previous.exists() else []
    terminal_release=False
    for old in old_sessions:
        c.e.require(old.is_dir() and not old.is_symlink() and (old/"acceptance.json").is_file(),"RESEARCH_EXPORT_PREVIOUS_SESSION_REQUIRES_REVIEW")
        p=c.load(old/"acceptance.json",65536);m=c.load(old/"execution.json",65536)
        c.e.require(JAVA_REPORT.fullmatch(p["report_relative_path"]),"RESEARCH_EXPORT_REPORT_PATH_INVALID")
        c.e.require(p["request_sha256"]==c.e.sha(root/"request.json") and p["execution_sha256"]==c.e.sha(old/"execution.json")
                    and p["report_sha256"]==c.e.sha(c.e.safe(root/p["report_relative_path"]))
                    and type(m["exit_code"]) is int and m["exit_code"]==0 and m["timed_out"] is False
                    and p["scientific_approval"] is False and type(p["lease_released"]) is bool
                    and p["status"]==("DATASET_EXPORTED" if p["lease_released"] else "CHECKPOINT_ACCEPTED"),
                    "RESEARCH_EXPORT_PREVIOUS_ACCEPTANCE_CHANGED")
        old_report=c.load(c.e.safe(root/p["report_relative_path"]),65536)
        c.e.require(old_report["request_sha256"]==p["request_sha256"] and old_report["runner_sha256"]==r["runner_sha256"]
                    and type(old_report["completed_steps"]) is int and 0<=old_report["completed_steps"]<=10
                    and p["lease_released"]==(old_report["status"]=="DATASET_EXPORTED" and old_report["completed_steps"]==10),
                    "RESEARCH_EXPORT_PREVIOUS_REPORT_CHANGED")
        terminal_release|=p["lease_released"]
    if held is None and old_sessions:
        c.e.require(terminal_release,"RESEARCH_EXPORT_CHECKPOINT_LEASE_MISSING")
    previous.mkdir(exist_ok=True,mode=0o700)
    session=previous/uuid.uuid4().hex;session.mkdir(mode=0o700)
    sys.path.insert(0,"/opt/airflow/dags");from source_run_guard import lease
    if held is None:lease("acquire",owner)
    before={p.name for p in (root/"sessions").iterdir()} if (root/"sessions").exists() else set()
    command=["/opt/spark/bin/spark-submit","--master","local[1]","--driver-memory","768m","--conf","spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m",
             "--jars",",".join(map(str,jars)),"--class",CLASS,str(root/"runner.jar"),str(root/"request.json"),str(seconds)]
    child=module("dataset_owned_child",HERE/"child-control.py");started=time.monotonic()
    with (session/"stdout.private").open("xb") as out,(session/"stderr.private").open("xb") as err:
        os.chmod(out.name,0o600);os.chmod(err.name,0o600);code,timed_out=child.bounded_child(command,out,err,seconds+120)
    c.e.save(session/"execution.json",{"exit_code":code,"timed_out":timed_out,"wall_seconds":round(time.monotonic()-started,3),
                                     "child_peak_rss_kib":resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss})
    c.e.require(code==0 and not timed_out,"RESEARCH_EXPORT_FAILED_OWN_LEASE_RETAINED");c.quiet(owner)
    after={p.name for p in (root/"sessions").iterdir()};new=after-before
    c.e.require(len(new)==1,"RESEARCH_EXPORT_EXACT_JAVA_REPORT_REQUIRED");report_path=c.e.safe(root/"sessions"/new.pop())
    c.e.require(JAVA_REPORT.fullmatch(report_path.relative_to(root).as_posix()),"RESEARCH_EXPORT_REPORT_PATH_INVALID")
    report=c.load(report_path,65536)
    c.e.require(report["request_sha256"]==c.e.sha(root/"request.json") and report["runner_sha256"]==r["runner_sha256"]
                and type(report["catalog_completed_months"]) is int and report["catalog_completed_months"]==288
                and report["synthetic_component_qa"] is False and report["scientific_approval"] is False
                and report["version"]==VERSION and report["dataset_split"]==r["dataset_split"]
                and type(report["completed_steps"]) is int and report["required_steps"]==10
                and re.fullmatch(r"ds_[a-f0-9]{64}",report["dataset_id"]),"RESEARCH_EXPORT_REPORT_CHANGED")
    released=False
    if report["status"]=="DATASET_EXPORTED":
        final=report["final_readback"]
        c.e.require(report["completed_steps"]==10 and final["status"]=="DATASET_EXPORTED" and final["trusted_handoff_verified"] is True
                    and final["quality_gate_status"]=="PASSED" and final["invalid_feature_count"]==0 and final["feature_row_count"]>0,"RESEARCH_EXPORT_HANDOFF_NOT_ACCEPTED")
        c.e.require(lease("release",owner)=={"status":"RELEASED",**owner},"RESEARCH_EXPORT_RELEASE_UNCONFIRMED");released=True
    else:c.e.require(report["status"]=="CHECKPOINTED" and 0<=report["completed_steps"]<10,"RESEARCH_EXPORT_REPORT_NOT_ACCEPTED")
    c.e.save(session/"acceptance.json",{"status":"DATASET_EXPORTED" if released else "CHECKPOINT_ACCEPTED","request_sha256":c.e.sha(root/"request.json"),
                                      "execution_sha256":c.e.sha(session/"execution.json"),"report_relative_path":report_path.relative_to(root).as_posix(),
                                      "report_sha256":c.e.sha(report_path),"lease_released":released,"scientific_approval":False})
    print("event=dat03_dataset_session_accepted completed="+str(report["completed_steps"])+" required=10",flush=True)


TRANSFER=r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-ml-export/[a-f0-9]{32}",str(p))
assert not any(q.is_symlink() for q in (p,*p.parents));files=json.loads(sys.stdin.buffer.readline())
assert len(files)==8 and {f["name"] for f in files}=={"runner.jar","request.json","feature-scaling.json","controller.py","catalog-control.py","research-e2e.py","child-control.py","catalog-acceptance-pin.json"}
assert sum(f["bytes"] for f in files)<=64*1024**2
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


def run(deployment,root,seconds,resume):
    c=support();deadline=time.monotonic()+7200;deployment,root=map(c.e.safe,(deployment,root))
    relative=root.relative_to(deployment/"staging/dat03-ml-export");c.e.require(re.fullmatch(r"[a-f0-9]{32}",str(relative)),"RESEARCH_EXPORT_EXACT_HOST_ROOT")
    c.e.require(type(seconds) is int and MIN_SECONDS<=seconds<=MAX_SECONDS,"RESEARCH_EXPORT_BUDGET_REQUIRED")
    pins=c.load(root/"inputs.json",65536);c.e.require(set(pins)==set(FILES),"RESEARCH_EXPORT_EXACT_INPUT_SET_REQUIRED")
    for name,item in pins.items():c.e.require(c.e.sha(root/name)==item["sha256"] and (root/name).stat().st_size==item["bytes"],"RESEARCH_EXPORT_PIN_CHANGED")
    r=c.module("research-rollout");r.services();r.trino_queries_idle()
    inside="/opt/pipeline/staging/dat03-ml-export/"+str(relative)
    if not resume:
        r.idle();c.e.require(not (root/"runtime").exists(),"RESEARCH_EXPORT_RESUME_REQUIRED")
        files=[{"name":name,**pins[name]} for name in FILES]
        with subprocess.Popen(["docker","exec","-i",c.e.CONTAINER,"python3","-c",TRANSFER,inside],stdin=subprocess.PIPE) as process:
            process.stdin.write((json.dumps(files)+"\n").encode())
            for name in FILES:
                with (root/name).open("rb") as src:shutil.copyfileobj(src,process.stdin,1024**2)
            process.stdin.close();c.e.require(process.wait(timeout=120)==0,"RESEARCH_EXPORT_STAGE_FAILED")
    else:c.e.require((root/"runtime").is_dir(),"RESEARCH_EXPORT_RETAINED_RUNTIME_REQUIRED")
    bootstrap='''import hashlib,json,runpy,sys
from pathlib import Path
p=Path(sys.argv[1]);pins=json.loads(sys.argv[2])
assert not any(q.is_symlink() for q in (p,*p.parents))
for name,pin in pins.items():
    file=p/name
    assert not file.is_symlink() and file.is_file() and file.stat().st_size==pin["bytes"]
    assert hashlib.file_digest(file.open("rb"),"sha256").hexdigest()==pin["sha256"]
sys.path.insert(0,str(p));sys.argv=[str(p/"controller.py"),"worker","--folder",str(p),"--seconds",sys.argv[3]]
runpy.run_path(str(p/"controller.py"),run_name="__main__")
'''
    try:
        seconds=min(seconds,int(deadline-time.monotonic())-600);c.e.require(seconds>=MIN_SECONDS,"RESEARCH_EXPORT_PREFLIGHT_BUDGET_SPENT")
        subprocess.run(["docker","exec",c.e.CONTAINER,"python3","-B","-X","pycache_prefix=/dev/null","-c",bootstrap,inside,json.dumps(pins),str(seconds)],check=True,timeout=seconds+300)
    finally:
        target=root/("runtime" if not (root/"runtime").exists() else "runtime-observation-"+uuid.uuid4().hex)
        subprocess.run(["docker","cp",c.e.CONTAINER+":"+inside,str(target)],check=True,timeout=180)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument("mode",choices=("prepare","run","resume","worker"))
    p.add_argument("--deployment-root",type=Path);p.add_argument("--catalog",type=Path);p.add_argument("--catalog-runtime",type=Path);p.add_argument("--catalog-session",type=Path)
    p.add_argument("--split",choices=("REPRODUCTION","EXTENSION"));p.add_argument("--folder",type=Path);p.add_argument("--seconds",type=int,default=6000);a=p.parse_args()
    if a.mode=="prepare":
        p.error("exact deployment/catalog/runtime/session/split required") if any(v is None for v in (a.deployment_root,a.catalog,a.catalog_runtime,a.catalog_session,a.split)) else None
        print(prepare(a.deployment_root,a.catalog,a.catalog_runtime,a.catalog_session,a.split))
    elif a.mode=="worker":worker(a.folder,a.seconds)
    else:run(a.deployment_root,a.folder,a.seconds,a.mode=="resume")


if __name__=="__main__":main()
