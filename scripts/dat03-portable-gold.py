#!/usr/bin/env python3
"""DAT-03 part2: scoped six-table Gold publication, pinned inputs and no automatic retry."""
import argparse
from datetime import datetime, timezone
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

HERE = Path(__file__).resolve().parent
CONTAINER = "japan-earthquake-etl-airflow-scheduler-1"
VERSION = "dat03-prepared-gold-v1"
NAMESPACE = "iceberg.dat03_unified_20261114"
DAG = "dat03_portable_gold"
CLASS = "ie212.earthquake.spark.gold.PortableGoldPublicationJob"


def module(name):
    spec = importlib.util.spec_from_file_location(name.replace('-', '_'), HERE / (name + '.py'))
    value = importlib.util.module_from_spec(spec); spec.loader.exec_module(value); return value


p = module('dat03-portable-capacity')
require, safe, sha, load, save = p.require, p.safe, p.sha, p.load, p.save


def operation(root):
    root = safe(root); require(re.fullmatch(r'[a-f0-9]{32}', root.name), 'EXACT_GOLD_OPERATION_REQUIRED')
    return root, '/opt/pipeline/staging/dat03-portable-gold/' + root.name


def prepare(root, evidence, expected):
    root, inside = operation(root); require(not root.exists(), 'NEW_GOLD_OPERATION_REQUIRED')
    require(sha(evidence) == expected, 'TRUSTED_PART1_EVIDENCE_CHANGED'); e = load(evidence)
    require(e['part1_status'] == 'ACCEPTED_PREPARED_INPUTS' and e['months'] == 288 and e['batches'] == 27
            and e['gold_published'] is False and e['official_export_ready'] is False, 'ACCEPTED_FULL_PREPARED_INPUT_REQUIRED')
    first = e['sessions'][0]
    for key in ('catalog', 'acceptance', 'candidate', 'report', 'execution'):
        require(sha(first[key]['path']) == first[key]['sha256'], 'PART1_ACCEPTANCE_CHANGED')
    catalog = load(first['catalog']['path'], 4 * 1024**2)
    require(first['catalog']['sha256'] == e['catalog_sha256'] and catalog['status'] == 'PREPARED', 'TRUSTED_CATALOG_SHA_REQUIRED')
    for flag in ('gold_published', 'official_export_ready', 'research_ready', 'ml_upstream_eligible'):
        require(catalog[flag] is False, 'PREPARED_IS_NOT_PUBLICATION')
    require(sha(e['request']['path']) == e['request']['sha256'], 'PART1_REQUEST_CHANGED'); original = load(e['request']['path'])
    payload_bytes = sum(f['byte_size'] for table in list(catalog['tables'].values()) + [catalog['source_links']] for f in table['files'])
    required = 4 * 1024**3 + 4 * payload_bytes + 2 * 1024**3
    require(shutil.disk_usage(root.parent).free >= max(10 * 1024**3, required), 'GOLD_PHYSICAL_CAPACITY_RESERVE_REQUIRED')
    root.mkdir(mode=0o700)
    runner = HERE.parent / 'spark/target/japan-earthquake-etl-runner.jar'
    for source, name in ((runner, 'runner.jar'), (Path(__file__), 'controller.py'), (HERE / 'dat03-portable-capacity.py', 'dat03-portable-capacity.py')):
        shutil.copyfile(source, root / name); os.chmod(root / name, 0o600)
    now = datetime.now(timezone.utc)
    proof = {'version': VERSION, 'input_contract': 'dat03-prepared-catalog-v1', 'namespace': NAMESPACE,
             'prepared_catalog_sha256': e['catalog_sha256'], 'prepared_request_sha256': catalog['request']['sha256'],
             'coverage_sha256': catalog['coverage']['sha256'], 'provenance_sha256': catalog['provenance']['sha256'],
             'source_cutoff_utc': original['source_cutoff_utc']}
    request = {'version': VERSION, 'run_id': 'dat03-gold-' + root.name, 'dag_id': DAG, 'namespace': NAMESPACE,
               'prepared_root': '/opt/pipeline/staging/dat03-portable-import/' + e['operation'] + '/prepared',
               'prepared_catalog_sha256': e['catalog_sha256'], 'prepared_input': proof,
               'expected_counts': {k: t['row_count'] for k, t in catalog['tables'].items()},
               'processing_date': now.date().isoformat(), 'processed_at_utc': now.isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
               'runner_sha256': sha(root / 'runner.jar')}
    save(root / 'request.json', request)
    files = [{'name': name, 'path': str(root / name), 'bytes': (root / name).stat().st_size, 'sha256': sha(root / name)}
             for name in ('runner.jar', 'controller.py', 'dat03-portable-capacity.py', 'request.json')]
    save(root / 'inputs.json', {'version': VERSION, 'inside': inside, 'request_sha256': sha(root / 'request.json'), 'files': files,
         'dependencies': {name: sha(runner.parent / 'iceberg' / name) for name in p.DEPS},
         'guard_dependencies': {name: sha(HERE.parent / 'airflow/dags' / name) for name in ('source_run_guard.py', 'jma_backfill_runtime.py')},
         'prepared_payload_bytes': payload_bytes, 'physical_start_required_bytes': max(10 * 1024**3, required), 'part1_evidence_sha256': expected})
    print(json.dumps({'folder': str(root), 'request_sha256': sha(root / 'request.json'), 'prepared_payload_bytes': payload_bytes,
                      'physical_start_required_bytes': max(10 * 1024**3, required), 'gold_writes': False}), flush=True)


def owner(request): return {'dag_id': DAG, 'run_id': request['run_id']}


def idle(expected):
    script = '''import json,os,sys
from pathlib import Path
from sqlalchemy import create_engine,text
from sqlalchemy.pool import NullPool
p=Path(os.environ['SOURCE_GUARD_ROOT'])/'owner.json';assert (json.loads(p.read_bytes()) if p.exists() else None)==json.loads(sys.argv[1]),'OWN_OR_EMPTY_SOURCE_LEASE_REQUIRED'
engine=create_engine(os.environ['AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'],poolclass=NullPool,connect_args={'connect_timeout':10,'options':'-c default_transaction_read_only=on -c statement_timeout=10000'})
try:
 with engine.connect() as c:assert c.execute(text("SELECT count(*) FROM dag_run WHERE state IN ('running','queued')")).scalar_one()==0,'ACTIVE_DAGRUN'
finally:engine.dispose()
for proc in Path('/proc').iterdir():
 if proc.name.isdigit():
  try:assert (proc/'comm').read_text().strip()!='java','ACTIVE_JAVA'
  except FileNotFoundError:pass
print('PREPARED_GOLD_IDLE')
'''
    v = subprocess.run(['docker', 'exec', CONTAINER, 'python3', '-c', script, json.dumps(expected)], capture_output=True, text=True, timeout=30)
    require(v.returncode == 0 and v.stdout.strip() == 'PREPARED_GOLD_IDLE', 'GOLD_RUNTIME_NOT_IDLE')


def worker(root, expected, session, mode, seconds, rerun_pin=None, continuation=None, continuation_sha=None):
    root, inside = operation(root); require(str(root) == inside and mode in ('preflight', 'run', 'rerun', 'recover')
        and re.fullmatch(r'[a-f0-9]{32}', session) and type(seconds) is int and 600 <= seconds <= 6000, 'BOUNDED_GOLD_WORKER_REQUIRED')
    if mode == 'recover':
        require(continuation is not None and re.fullmatch(re.escape(inside) + r'/continuations/[a-f0-9]{32}/continuation.json', str(continuation))
                and sha(continuation) == continuation_sha, 'EXPLICIT_NARROW_CONTINUATION_REQUIRED')
        c = load(continuation)
        require(c['request_sha256'] == load(root / 'inputs.json')['request_sha256']
                and sha(Path(continuation).parent / 'runner.jar') == c['continuation_runner_sha256'], 'CONTINUATION_INPUT_CHANGED')
    require(sha(root / 'inputs.json') == expected, 'GOLD_INPUT_LIST_CHANGED'); inputs = load(root / 'inputs.json')
    for f in inputs['files']: require(sha(root / f['name']) == f['sha256'], 'GOLD_WORKER_INPUT_CHANGED')
    for name, digest in inputs['dependencies'].items(): require(sha(Path('/opt/spark/jobs/iceberg') / name) == digest, 'GOLD_RUNTIME_DEPENDENCY_CHANGED')
    for name, digest in inputs['guard_dependencies'].items(): require(sha(Path('/opt/airflow/dags') / name) == digest, 'GOLD_SOURCE_GUARD_CODE_CHANGED')
    request = load(root / 'request.json'); require(sha(root / 'request.json') == inputs['request_sha256'], 'GOLD_REQUEST_CHANGED')
    if mode == 'rerun': require(isinstance(rerun_pin, str) and re.fullmatch(r'[a-f0-9]{64}', rerun_pin)
            and sha(root / 'accepted-pins.json') == rerun_pin, 'TRUSTED_GOLD_RERUN_PINS_REQUIRED')
    sys.path.insert(0, '/opt/airflow/dags'); from source_run_guard import lease
    attempt = root / 'sessions' / session; attempt.mkdir(parents=True, mode=0o700)
    phases = ['preflight'] if mode == 'preflight' else ['commit', 'publication']
    if mode != 'preflight': lease('assert' if mode == 'recover' else 'acquire', owner(request))
    started = time.monotonic(); records = []
    for phase in phases:
        remaining = seconds - (time.monotonic() - started)
        require(remaining >= 300, 'GOLD_PHASE_BUDGET_INSUFFICIENT_LEASE_RETAINED')
        java_phase = 'recover-' + phase if mode == 'recover' else phase + ('-rerun' if mode == 'rerun' else '')
        common = [str(root / 'request.json'), inputs['request_sha256'], java_phase, session]
        if mode == 'rerun': common.extend([str(root / 'accepted-pins.json'), rerun_pin])
        runner = root / 'runner.jar'
        if mode == 'recover':
            common.extend([str(continuation), continuation_sha]); runner = Path(continuation).parent / 'runner.jar'
        if phase == 'commit':
            command = ['/opt/spark/bin/spark-submit', '--master', 'local[1]', '--driver-memory', '768m', '--conf',
                'spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m', '--jars', ','.join('local:/opt/spark/jobs/iceberg/' + n for n in p.DEPS),
                '--class', CLASS, 'local:' + str(runner), *common]
        else:
            command = ['java', '-Xmx512m', '-cp', str(runner) + ':/opt/spark/jars/*:/opt/spark/jobs/iceberg/*', CLASS, *common]
        with (attempt / (phase + '-stdout.private')).open('xb') as out, (attempt / (phase + '-stderr.private')).open('xb') as err:
            os.chmod(out.name, 0o600); os.chmod(err.name, 0o600); child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
            save(attempt / (phase + '-active.json'), {'pid': child.pid, 'phase': phase, 'run_id': request['run_id']})
            stop = None
            while child.poll() is None:
                if time.monotonic() - started > seconds: stop = 'SESSION_DEADLINE'; break
                if (root / ('stop-' + session + '.json')).exists(): stop = 'HOST_PHYSICAL_DISK_RESERVE'; break
                time.sleep(2)
            if stop:
                os.killpg(child.pid, signal.SIGTERM)
                try: child.wait(timeout=20)
                except subprocess.TimeoutExpired: os.killpg(child.pid, signal.SIGKILL); child.wait(timeout=10)
            execution = {'exit_code': child.wait(), 'stop_reason': stop, 'automatic_retry': False,
                         'elapsed_session_seconds': round(time.monotonic() - started, 3), 'phase': phase}
        save(attempt / (phase + '-execution.json'), execution)
        require(execution['exit_code'] == 0 and stop is None, 'GOLD_PHASE_FAILED_RETAIN_LEASE_AND_CHECKPOINT')
        result_path = root / 'results' / (session + '-' + phase + '.json'); value = load(result_path, 4 * 1024**2)
        require(value['request_sha256'] == inputs['request_sha256'] and value['run_id'] == request['run_id'], 'GOLD_PHASE_RESULT_CHANGED')
        if mode == 'rerun': require(value['reused'] is True, 'GOLD_RERUN_MUST_REUSE_EXISTING_COMMIT_AND_PUBLICATION')
        # Retain immutable full phase receipts locally without duplicating warehouse payload.
        save(attempt / (phase + '-result.json'), value)
        records.append({'phase': phase, 'result': value, 'result_sha256': sha(attempt / (phase + '-result.json')),
                        'execution_sha256': sha(attempt / (phase + '-execution.json'))})
    save(attempt / 'candidate.json', {'version': VERSION, 'mode': mode, 'session': session, 'request_sha256': inputs['request_sha256'],
         'records': records, 'status': records[-1]['result']['status'], 'lease_retained': mode != 'preflight'})


def verify_rerun(first, current):
    require(first['status'] == current['status'] == 'PUBLISHED', 'PUBLISHED_RERUN_REQUIRED')
    for a, b in zip(first['records'], current['records'], strict=True):
        require(a['phase'] == b['phase'] and b['result']['reused'] is True and a['result']['commit_sha256'] == b['result']['commit_sha256'], 'IMMUTABLE_GOLD_COMMIT_CHANGED_ON_RERUN')
        if b['phase'] == 'publication':
            require(a['result']['publication_sha256'] == b['result']['publication_sha256'] and a['result']['snapshots'] == b['result']['snapshots'], 'PUBLICATION_OR_SNAPSHOTS_CHANGED_ON_RERUN')


def capture(root, session):
    root, inside = operation(root); inputs = load(root / 'inputs.json'); request = load(root / 'request.json')
    host = root / 'host-sessions' / session; intent = load(host / 'intent.json')
    require(intent['inputs_sha256'] == sha(root / 'inputs.json') and intent['session'] == session and not (host / 'runtime').exists(), 'GOLD_HOST_INTENT_CHANGED_OR_PARTIAL_CAPTURE')
    mode = intent['mode']; r = module('research-rollout'); idle(None if mode == 'preflight' else owner(request)); r.trino_queries_idle()
    require(r.services() == intent['services_before'], 'GOLD_SERVICES_CHANGED')
    subprocess.run(['docker', 'cp', CONTAINER + ':' + inside + '/sessions/' + session, str(host / 'runtime')], check=True, timeout=120)
    candidate = load(host / 'runtime/candidate.json', 4 * 1024**2)
    require(candidate['session'] == session and candidate['mode'] == mode and candidate['request_sha256'] == inputs['request_sha256']
            and candidate['status'] == ('READ_ONLY_PREFLIGHT_PASSED' if mode == 'preflight' else 'PUBLISHED'), 'GOLD_TERMINAL_CANDIDATE_REQUIRED')
    for record in candidate['records']:
        phase = record['phase']; require(sha(host / 'runtime' / (phase + '-result.json')) == record['result_sha256']
            and sha(host / 'runtime' / (phase + '-execution.json')) == record['execution_sha256'], 'GOLD_HOST_RECEIPT_CAPTURE_CHANGED')
    if mode == 'rerun': verify_rerun(load(root / 'first-published.json', 4 * 1024**2), candidate)
    if mode != 'preflight':
        script = 'import sys,json;sys.path.insert(0,"/opt/airflow/dags");from source_run_guard import lease;p=json.loads(sys.argv[1]);lease("assert",p);print(json.dumps(lease("release",p)))'
        v = subprocess.run(['docker', 'exec', CONTAINER, 'python3', '-c', script, json.dumps(owner(request))], capture_output=True, text=True, check=True, timeout=20)
        require(json.loads(v.stdout) == {'status': 'RELEASED', **owner(request)}, 'GOLD_OWN_SOURCE_RELEASE_NOT_CONFIRMED')
    idle(None); r.trino_queries_idle(); require(r.services() == intent['services_before'], 'GOLD_POST_RELEASE_SERVICES_CHANGED')
    save(host / 'acceptance.json', {'version': VERSION, 'status': candidate['status'], 'mode': mode, 'candidate_sha256': sha(host / 'runtime/candidate.json'),
         'request_sha256': inputs['request_sha256'], 'inputs_sha256': sha(root / 'inputs.json'), 'services_unchanged': True,
         'source_lease_empty': True, 'official_export_ready': False, 'controller_sha256': sha(Path(__file__))})
    if mode == 'preflight': save(root / 'preflight-accepted.json', {'session': session, 'acceptance_sha256': sha(host / 'acceptance.json')})
    if mode in ('run', 'recover'):
        save(root / 'first-published.json', candidate)
        publication = candidate['records'][-1]['result']
        save(root / 'accepted-pins.json', {'request_sha256': inputs['request_sha256'], 'commit_sha256': publication['commit_sha256'],
             'publication_sha256': publication['publication_sha256'], 'snapshots': publication['snapshots'],
             'source_acceptance_sha256': sha(host / 'acceptance.json'), 'source_session': session})
    print(json.dumps({'event': 'portable_gold_session_accepted', 'mode': mode, 'session': session, 'status': candidate['status'],
                      'records': [{'phase': v['phase'], 'status': v['result']['status'], 'commit_sha256': v['result'].get('commit_sha256'),
                                   'verify_queries': v['result'].get('verify_queries'), 'publish_queries': v['result'].get('publish_queries')} for v in candidate['records']]}), flush=True)


def resolved_session(root, folder):
    if (folder / 'acceptance.json').is_file(): return True
    resolution = folder / 'resolution.json'
    if not resolution.is_file(): return False
    v = load(resolution)
    require(isinstance(v.get('recovery_session'), str) and re.fullmatch(r'[a-f0-9]{32}', v['recovery_session']), 'EXACT_RECOVERY_SESSION_REQUIRED')
    accepted = root / 'host-sessions' / v['recovery_session'] / 'acceptance.json'; a = load(accepted)
    require(v['failed_execution_sha256'] == sha(folder / 'failed-runtime/commit-execution.json')
            and v['recovery_acceptance_sha256'] == sha(accepted)
            and a['status'] == 'PUBLISHED' and a['mode'] == 'recover' and a['source_lease_empty'] is True
            and a['candidate_sha256'] == sha(accepted.parent / 'runtime/candidate.json')
            and a['request_sha256'] == v['request_sha256'] == sha(root / 'request.json'), 'RECOVERY_RESOLUTION_CHANGED')
    return True


def run(root, mode, seconds):
    root, inside = operation(root); inputs = load(root / 'inputs.json'); r = module('research-rollout')
    require(mode in ('preflight', 'run', 'rerun') and type(seconds) is int and 600 <= seconds <= 6000, 'BOUNDED_GOLD_SESSION_REQUIRED')
    require(shutil.disk_usage(root).free >= inputs['physical_start_required_bytes'], 'GOLD_HOST_DISK_CAPACITY_REQUIRED')
    before = r.services(); idle(None); r.trino_queries_idle()
    if mode == 'preflight':
        require(not (root / 'staged.json').exists(), 'ALREADY_STAGED_NO_AUTO_RETRY')
        transfer = module('dat03-portable-import').TRANSFER.replace('dat03-portable-import', 'dat03-portable-gold')
        files = inputs['files'] + [{'name': 'inputs.json', 'path': str(root / 'inputs.json'), 'bytes': (root / 'inputs.json').stat().st_size, 'sha256': sha(root / 'inputs.json')}]
        with subprocess.Popen(['docker', 'exec', '-i', CONTAINER, 'python3', '-c', transfer, inside], stdin=subprocess.PIPE) as child:
            child.stdin.write((json.dumps([{k: v for k, v in f.items() if k != 'path'} for f in files]) + '\n').encode())
            for f in files:
                require(sha(f['path']) == f['sha256'], 'GOLD_STAGE_ASSET_CHANGED')
                with safe(f['path']).open('rb') as stream: shutil.copyfileobj(stream, child.stdin, 1024**2)
            child.stdin.close(); require(child.wait(timeout=120) == 0, 'GOLD_STAGE_FAILED_NO_RETRY')
        save(root / 'staged.json', {'inputs_sha256': sha(root / 'inputs.json')})
    else:
        require(load(root / 'staged.json')['inputs_sha256'] == sha(root / 'inputs.json'), 'GOLD_STAGED_INPUT_CHANGED')
        preflight = load(root / 'preflight-accepted.json'); require(sha(root / 'host-sessions' / preflight['session'] / 'acceptance.json') == preflight['acceptance_sha256'], 'GOLD_READ_ONLY_PREFLIGHT_REQUIRED')
        for folder in (root / 'host-sessions').iterdir(): require(resolved_session(root, folder), 'UNACCEPTED_GOLD_SESSION_NO_AUTO_RETRY')
        require((root / 'first-published.json').exists() == (mode == 'rerun'), 'EXPLICIT_GOLD_RUN_OR_RERUN_REQUIRED')
    rerun_pin = ''
    if mode == 'rerun':
        pins = load(root / 'accepted-pins.json'); first_host = root / 'host-sessions' / pins['source_session']
        require(sha(first_host / 'acceptance.json') == pins['source_acceptance_sha256']
                and load(first_host / 'acceptance.json')['candidate_sha256'] == sha(root / 'first-published.json'), 'GOLD_EXTERNAL_ACCEPTANCE_CHANGED')
        rerun_pin = sha(root / 'accepted-pins.json')
        send = 'from pathlib import Path;import sys,hashlib;p=Path(sys.argv[1]);raw=sys.stdin.buffer.read(65537);assert len(raw)<=65536 and hashlib.sha256(raw).hexdigest()==sys.argv[2];assert not p.is_symlink();p.write_bytes(raw) if not p.exists() else None;assert hashlib.sha256(p.read_bytes()).hexdigest()==sys.argv[2]'
        subprocess.run(['docker', 'exec', '-i', CONTAINER, 'python3', '-c', send, inside + '/accepted-pins.json', rerun_pin], input=(root / 'accepted-pins.json').read_bytes(), check=True, timeout=30)
    session = uuid.uuid4().hex; host = root / 'host-sessions' / session; host.mkdir(parents=True, mode=0o700)
    save(host / 'intent.json', {'session': session, 'mode': mode, 'inputs_sha256': sha(root / 'inputs.json'), 'services_before': before})
    bootstrap = 'import hashlib,sys,runpy;from pathlib import Path;p=Path(sys.argv[1]);assert hashlib.sha256((p/"controller.py").read_bytes()).hexdigest()==sys.argv[2];assert hashlib.sha256((p/"dat03-portable-capacity.py").read_bytes()).hexdigest()==sys.argv[3];sys.argv=[str(p/"controller.py"),"worker","--folder",str(p),"--inputs-sha256",sys.argv[4],"--session",sys.argv[5],"--worker-mode",sys.argv[6],"--seconds",sys.argv[7],"--rerun-pins-sha256",sys.argv[8]];runpy.run_path(str(p/"controller.py"),run_name="__main__")'
    with (host / 'worker.private').open('xb') as log:
        child = subprocess.Popen(['docker', 'exec', CONTAINER, 'python3', '-c', bootstrap, inside, sha(root / 'controller.py'), sha(root / 'dat03-portable-capacity.py'), sha(root / 'inputs.json'), session, mode, str(seconds), rerun_pin], stdout=log, stderr=subprocess.STDOUT)
        started = time.monotonic(); last = None; stop_written = False
        while child.poll() is None:
            require(time.monotonic() - started <= seconds + 180, 'GOLD_HOST_DEADLINE_UNKNOWN_KEEP_LEASE')
            if shutil.disk_usage(root).free < 4 * 1024**3 and not stop_written:
                stop = 'from pathlib import Path;import sys;p=Path(sys.argv[1]);p.write_bytes(b"{\"stop\":true}")'
                subprocess.run(['docker', 'exec', CONTAINER, 'python3', '-c', stop, inside + '/stop-' + session + '.json'], check=True, timeout=20); stop_written = True
            probe = 'import sys,re;from pathlib import Path;p=Path(sys.argv[1]);v=[]\nfor name in ("preflight","commit","publication"):\n f=p/(name+"-stdout.private")\n if f.exists():v.extend(x for x in f.read_text(errors="replace").splitlines() if re.fullmatch(r"event=prepared_gold_[a-z_]+ run_id=dat03-gold-[a-f0-9]{32} (?:phase=[a-z]+ query=[0-9]+/[0-9]+|table=[a-z_]+|phase=[a-z]+ status=[A-Z_]+)",x))\nprint(v[-1] if v else "event=prepared_gold_initial_verification")'
            try:
                value = subprocess.run(['docker', 'exec', CONTAINER, 'python3', '-c', probe, inside + '/sessions/' + session], capture_output=True, text=True, timeout=20)
                if value.returncode == 0 and value.stdout.strip() != last: last = value.stdout.strip(); print(last, flush=True)
            except subprocess.TimeoutExpired: pass  # Optional observation is not worker outcome.
            time.sleep(15)
        require(child.wait() == 0 and not stop_written, 'GOLD_SESSION_FAILED_KEEP_CHECKPOINT_AND_OWN_LEASE')
    capture(root, session)


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('mode', choices=('prepare', 'preflight', 'run', 'rerun', 'worker', 'capture'))
    parser.add_argument('--folder', type=Path, required=True); parser.add_argument('--evidence', type=Path); parser.add_argument('--evidence-sha256')
    parser.add_argument('--seconds', type=int, default=6000); parser.add_argument('--session'); parser.add_argument('--inputs-sha256'); parser.add_argument('--worker-mode')
    parser.add_argument('--rerun-pins-sha256')
    parser.add_argument('--continuation', type=Path); parser.add_argument('--continuation-sha256')
    a = parser.parse_args()
    if a.mode == 'prepare': prepare(a.folder, a.evidence, a.evidence_sha256)
    elif a.mode == 'worker': worker(a.folder, a.inputs_sha256, a.session, a.worker_mode, a.seconds, a.rerun_pins_sha256, a.continuation, a.continuation_sha256)
    elif a.mode == 'capture': capture(a.folder, a.session)
    else: run(a.folder, a.mode, a.seconds)


if __name__ == '__main__': main()
