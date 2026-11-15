#!/usr/bin/env python3
"""EXPLICIT Java17/Spark whole-window 40k synthetic resource benchmark; no S3/ML writes."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('prepared_gold', HERE / 'dat03-portable-gold.py')
g = importlib.util.module_from_spec(spec); spec.loader.exec_module(g)

WORKER = r'''
import json,hashlib,os,subprocess,sys,time,signal,resource
from pathlib import Path
p=Path(sys.argv[1]);assets=json.loads(sys.argv[2]);assert p.as_posix().startswith('/opt/pipeline/staging/dat03-40k-profile/')
for name,digest in assets.items():
 f=p/name;assert not f.is_symlink() and hashlib.sha256(f.read_bytes()).hexdigest()==digest
started=time.monotonic();jars='local:'+str(p/'runner.jar')+',local:/opt/spark/jobs/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar,local:/opt/spark/jobs/iceberg/iceberg-aws-bundle-1.10.1.jar'
command=['/opt/spark/bin/spark-submit','--master','local[1]','--driver-memory','768m','--conf','spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m','--jars',jars,'--class','ie212.earthquake.spark.ml.PreparedMl40kProfileJob','local:'+str(p/'runner.jar'),str(p/'scratch'),str(p/'runner.jar')]
(p/'scratch').mkdir(mode=0o700)
import shutil
shutil.copyfile(p/'feature-scaling.json',p/'scratch/feature-scaling.json')
with (p/'stdout.private').open('xb') as out,(p/'stderr.private').open('xb') as err:
 os.chmod(out.name,0o600);os.chmod(err.name,0o600);child=subprocess.Popen(command,stdout=out,stderr=err,start_new_session=True);timed=False
 try:code=child.wait(timeout=600)
 except subprocess.TimeoutExpired:
  timed=True;os.killpg(child.pid,signal.SIGTERM)
  try:code=child.wait(timeout=20)
  except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);code=child.wait(timeout=10)
result={'exit_code':code,'timed_out':timed,'wall_seconds':round(time.monotonic()-started,3),'child_peak_rss_kib':resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,'synthetic_component_qa':True,'s3_writes':False}
with (p/'execution.json').open('x') as f:json.dump(result,f)
print(json.dumps(result));sys.exit(code or (1 if timed else 0))
'''


def run(root):
    root = g.safe(root); g.require(root.name and len(root.name) == 32 and all(c in 'abcdef0123456789' for c in root.name) and not root.exists(), 'NEW_EXACT_40K_PROFILE_ROOT_REQUIRED')
    g.require(shutil.disk_usage(root.parent).free >= 10 * 1024**3, 'PROFILE_PHYSICAL_RESERVE_REQUIRED')
    r = g.module('research-rollout'); before = r.services(); g.idle(None); r.trino_queries_idle()
    root.mkdir(mode=0o700)
    repo = HERE.parent
    shutil.copyfile(repo / 'spark/target/japan-earthquake-etl-runner.jar', root / 'runner.jar')
    shutil.copyfile(repo / 'config/ml/feature_scaling_global_research_40k_v1.json', root / 'feature-scaling.json')
    assets = {name: g.sha(root / name) for name in ('runner.jar', 'feature-scaling.json')}
    g.save(root / 'intent.json', {'assets': assets, 'worker_source_sha256': hashlib.sha256(WORKER.encode()).hexdigest(), 'services_before': before, 's3_writes': False})
    inside = '/opt/pipeline/staging/dat03-40k-profile/' + root.name
    subprocess.run(['docker', 'exec', g.CONTAINER, 'mkdir', '-p', '-m', '700', inside], check=True)
    for name in assets: subprocess.run(['docker', 'cp', str(root / name), g.CONTAINER + ':' + inside + '/' + name], check=True)
    run = subprocess.run(['docker', 'exec', g.CONTAINER, 'python3', '-B', '-c', WORKER, inside, json.dumps(assets)], capture_output=True, text=True, timeout=660)
    subprocess.run(['docker', 'cp', g.CONTAINER + ':' + inside, str(root / 'runtime')], check=True, timeout=120)
    g.idle(None); r.trino_queries_idle(); g.require(r.services() == before, 'PROFILE_SERVICES_CHANGED')
    execution = g.load(root / 'runtime/execution.json')
    if run.returncode != 0:
        print(json.dumps({'status': 'PROFILE_FAILED', 'folder': str(root), 'execution': execution, 'retry': False}), flush=True)
        raise ValueError('PROFILE_FAILED_REVIEW_RETAINED_PRIVATE_LOGS')
    result = g.load(root / 'runtime/scratch/result.json')
    g.require(execution['exit_code'] == 0 and execution['timed_out'] is False and result['status'] == 'PROFILE_BENCHMARK_PASSED'
            and result['runner_sha256'] == assets['runner.jar'] and result['candidate_semantics_unchanged'] is True
            and result['feature_rows'] == 40000 and result['feature_parquet_bytes'] <= 64*1024**2, 'MEASURED_PROFILE_NOT_ACCEPTED')
    result.update(execution_sha256=g.sha(root / 'runtime/execution.json'), result_sha256=g.sha(root / 'runtime/scratch/result.json'),
                  worker_source_sha256=hashlib.sha256(WORKER.encode()).hexdigest(),
                  wall_seconds=execution['wall_seconds'], child_peak_rss_kib=execution['child_peak_rss_kib'], services_unchanged=True)
    g.save(root / 'profile-receipt.json', result)
    print(json.dumps({'status': 'PROFILE_BENCHMARK_PASSED', 'receipt': str(root / 'profile-receipt.json'), 'sha256': g.sha(root / 'profile-receipt.json'),
                      'wall_seconds': execution['wall_seconds'], 'child_peak_rss_kib': execution['child_peak_rss_kib']}), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__); p.add_argument('--folder', type=Path, required=True); a=p.parse_args();run(a.folder)
