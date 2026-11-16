#!/usr/bin/env python3
"""Official ML from accepted Published-PREPARED Gold; scoped checkpoints, no automatic failure retry."""
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
def module(name):
    s = importlib.util.spec_from_file_location(name.replace('-', '_'), HERE / (name + '.py'))
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
g = module('dat03-portable-gold')
require, sha, load, save, safe = g.require, g.sha, g.load, g.save, g.safe
VERSION = 'dat03-prepared-ml-v2'
CLASS = 'ie212.earthquake.spark.ml.PreparedDatasetBuildJob'
SPLITS = ('REPRODUCTION', 'EXTENSION')
PROFILES = {'dat03-whole-window-40k-v1': (40000, 'feature_scaling_global_research_40k_v1.json'),
            'dat03-whole-window-90k-v1': (90000, 'feature_scaling_global_research_90k_v1.json')}


def verify_profile(benchmark, runner_sha, scaling_sha):
    require(benchmark.get('profile_version') in PROFILES, 'APPROVED_PROFILE_REQUIRED')
    cap, _ = PROFILES[benchmark['profile_version']]
    require(benchmark.get('status') == 'PROFILE_BENCHMARK_PASSED' and type(benchmark.get('feature_rows')) is int
            and benchmark['feature_rows'] == cap and benchmark.get('candidate_semantics_unchanged') is True
            and benchmark.get('synthetic_component_qa') is True and benchmark.get('s3_writes') is False
            and benchmark.get('scientific_approval') is False and benchmark.get('official_exported') is False
            and benchmark.get('null_guard_fault_injections_passed') == 2 and benchmark.get('services_unchanged') is True
            and benchmark.get('runner_sha256') == runner_sha and benchmark.get('scaling_sha256') == scaling_sha,
            'VERIFIED_PROFILE_NOT_READY')


def operation(root):
    root = safe(root); require(re.fullmatch(r'[a-f0-9]{32}', root.name), 'EXACT_PREPARED_ML_OPERATION_REQUIRED')
    return root, '/opt/pipeline/staging/dat03-prepared-ml/' + root.name


def owner(root): return {'dag_id': 'mli_02_build_dataset', 'run_id': 'dat03-ml-' + root.name}


def require_resume_context(intent, split, mode):
    require(intent.get('split') == split and intent.get('mode') == mode == 'build', 'ML_RETAINED_CHECKPOINT_SAME_SPLIT_BUILD_REQUIRED')


def prepare(root, evidence, expected, profile=None, profile_sha=None):
    root, inside = operation(root); require(not root.exists() and sha(evidence) == expected, 'NEW_OPERATION_AND_TRUSTED_GOLD_EVIDENCE_REQUIRED')
    e = load(evidence); require(e['part2_status'] == 'ACCEPTED_PUBLISHED_IMMUTABLE_RERUN' and e['months'] == 288
        and e['gold_published'] is True and e['official_export_ready'] is False, 'ACCEPTED_FULL_GOLD_WITH_IMMUTABLE_RERUN_REQUIRED')
    gold = HERE.parent / 'staging/dat03-portable-gold' / e['operation']
    require(sha(gold / 'inputs.json') == e['inputs_sha256'] and sha(gold / 'request.json') == e['request_sha256'], 'GOLD_FROZEN_INPUT_CHANGED')
    for k in ('first_publication', 'rerun'):
        receipt = e[k]; p = gold / 'host-sessions' / receipt['session']; a = load(p / 'acceptance.json')
        require(sha(p / 'acceptance.json') == receipt['acceptance_sha256'] and a['candidate_sha256'] == sha(p / 'runtime/candidate.json')
                and a['status'] == 'PUBLISHED' and a['source_lease_empty'] is True, 'GOLD_RUNTIME_ACCEPTANCE_CHANGED')
    g.verify_rerun(load(gold / 'first-published.json', 4 * 1024**2), load(gold / 'host-sessions' / e['rerun']['session'] / 'runtime/candidate.json', 4 * 1024**2))
    require(profile is not None and sha(profile) == profile_sha, 'MEASURED_EXTERNAL_PROFILE_REQUIRED')
    benchmark = load(profile)
    require(benchmark.get('profile_version') in PROFILES, 'APPROVED_PROFILE_REQUIRED')
    scaling = HERE.parent / 'config/ml' / PROFILES[benchmark['profile_version']][1]
    verify_profile(benchmark, sha(HERE.parent / 'spark/target/japan-earthquake-etl-runner.jar'), sha(scaling))
    require(shutil.disk_usage(root.parent).free >= 12 * 1024**3, 'PREPARED_ML_START_RESERVE_12GIB_REQUIRED')
    root.mkdir(mode=0o700)
    for source, name in ((HERE.parent / 'spark/target/japan-earthquake-etl-runner.jar', 'runner.jar'), (Path(__file__), 'controller.py'),
                         (HERE / 'dat03-portable-gold.py', 'dat03-portable-gold.py'), (HERE / 'dat03-portable-capacity.py', 'dat03-portable-capacity.py'),
                         (scaling, 'feature-scaling.json'), (safe(profile), 'resource-profile.json')):
        shutil.copyfile(source, root / name); os.chmod(root / name, 0o600)
    save(root / 'request.json', {'version': VERSION, 'runner_sha256': sha(root / 'runner.jar'), 'gold_run_id': 'dat03-gold-' + e['operation'],
         'gold_commit_sha256': e['first_publication']['commit_sha256'], 'gold_publication_sha256': e['first_publication']['publication_sha256'],
         'scaling_sha256': sha(root / 'feature-scaling.json'), 'observation_cutoff_utc': datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
         'resource_profile_version': benchmark['profile_version'], 'profile_receipt_sha256': profile_sha})
    before = load(gold / 'inputs.json')
    save(root / 'inputs.json', {'version': VERSION, 'inside': inside, 'request_sha256': sha(root / 'request.json'), 'gold_evidence_sha256': expected,
         'files': [{'name': n, 'bytes': (root / n).stat().st_size, 'sha256': sha(root / n)} for n in
                   ('runner.jar', 'controller.py', 'dat03-portable-gold.py', 'dat03-portable-capacity.py', 'feature-scaling.json', 'resource-profile.json', 'request.json')],
         'dependencies': before['dependencies'], 'guard_dependencies': before['guard_dependencies']})
    print(json.dumps({'folder': str(root), 'request_sha256': sha(root / 'request.json'), 'ml_writes': False}), flush=True)


def validate_assets(root, expected):
    require(sha(root / 'inputs.json') == expected, 'PREPARED_ML_INPUT_LIST_CHANGED'); inputs = load(root / 'inputs.json')
    for f in inputs['files']: require(sha(root / f['name']) == f['sha256'], 'PREPARED_ML_ASSET_CHANGED')
    require(sha(root / 'request.json') == inputs['request_sha256'], 'PREPARED_ML_REQUEST_CHANGED')
    return inputs


def worker(root, expected, split, mode, session, seconds, gate):
    root, inside = operation(root)
    require(str(root) == inside and split in SPLITS and mode in ('preflight', 'build', 'rerun') and re.fullmatch(r'[a-f0-9]{32}', session)
            and type(seconds) is int and 600 <= seconds <= 6000, 'BOUNDED_PREPARED_ML_WORKER_REQUIRED')
    inputs = validate_assets(root, expected)
    for name, digest in inputs['dependencies'].items(): require(sha(Path('/opt/spark/jobs/iceberg') / name) == digest, 'ML_DEPENDENCY_CHANGED')
    for name, digest in inputs['guard_dependencies'].items(): require(sha(Path('/opt/airflow/dags') / name) == digest, 'ML_SOURCE_GUARD_CHANGED')
    if mode != 'preflight': require(re.fullmatch(r'[a-f0-9]{64}', gate) and sha(root / split / 'preflight-accepted.json') == gate, 'ML_EXTERNAL_PREFLIGHT_PIN_REQUIRED')
    sys.path.insert(0, '/opt/airflow/dags'); from source_run_guard import lease
    held = Path(os.environ['SOURCE_GUARD_ROOT']) / 'owner.json'
    if held.exists(): lease('assert', owner(root))
    else: lease('acquire', owner(root))
    attempt = root / 'sessions' / session; attempt.mkdir(parents=True, mode=0o700)
    command = ['/opt/spark/bin/spark-submit', '--master', 'local[1]', '--driver-memory', '768m', '--conf',
        'spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m', '--jars', ','.join('local:/opt/spark/jobs/iceberg/' + n for n in g.p.DEPS),
        '--class', CLASS, 'local:' + str(root / 'runner.jar'), str(root / 'request.json'), inputs['request_sha256'], split, mode, session, str(seconds), gate or '-']
    started = time.monotonic(); stop = None
    with (attempt / 'stdout.private').open('xb') as out, (attempt / 'stderr.private').open('xb') as err:
        os.chmod(out.name, 0o600); os.chmod(err.name, 0o600); child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        save(attempt / 'active.json', {'pid': child.pid, 'split': split, 'mode': mode})
        while child.poll() is None:
            if time.monotonic() - started > seconds: stop = 'SESSION_DEADLINE'; break
            if (root / ('stop-' + session + '.json')).exists(): stop = 'HOST_PHYSICAL_RESERVE'; break
            time.sleep(2)
        if stop:
            os.killpg(child.pid, signal.SIGTERM)
            try: child.wait(timeout=20)
            except subprocess.TimeoutExpired: os.killpg(child.pid, signal.SIGKILL); child.wait(timeout=10)
        save(attempt / 'execution.json', {'exit_code': child.wait(), 'stop_reason': stop, 'automatic_retry': False, 'wall_seconds': round(time.monotonic() - started, 3)})
    require(child.returncode == 0 and stop is None, 'PREPARED_ML_FAILED_KEEP_LEASE_AND_EVIDENCE')
    source = root / 'results' / (session + '.json'); result = load(source, 8 * 1024**2)
    require(result['request_sha256'] == inputs['request_sha256'] and result['dataset_split'] == split and result['mode'] == mode, 'ML_RESULT_CONTEXT_CHANGED')
    shutil.copyfile(source, attempt / 'report.json'); os.chmod(attempt / 'report.json', 0o600)
    save(attempt / 'candidate.json', {'session': session, 'mode': mode, 'split': split, 'request_sha256': inputs['request_sha256'],
        'report_sha256': sha(attempt / 'report.json'), 'execution_sha256': sha(attempt / 'execution.json'), 'status': result['status']})


def run(root, split, mode, seconds):
    root, inside = operation(root); require(split in SPLITS and mode in ('preflight', 'build', 'rerun') and type(seconds) is int and 600 <= seconds <= 6000, 'BOUNDED_ML_SESSION_REQUIRED')
    inputs = validate_assets(root, sha(root / 'inputs.json')); r = g.module('research-rollout')
    require(shutil.disk_usage(root).free >= 10 * 1024**3, 'ML_PHYSICAL_CAPACITY_REQUIRED')
    before = r.services(); previous = list((root / 'host-sessions').iterdir()) if (root / 'host-sessions').exists() else []
    retained = False
    for p in previous:
        require((p / 'acceptance.json').is_file(), 'ML_UNACCEPTED_SESSION_NO_AUTO_RETRY')
        a = load(p / 'acceptance.json'); require(a['report_sha256'] == sha(p / 'runtime/report.json') and a['candidate_sha256'] == sha(p / 'runtime/candidate.json')
            and a['execution_sha256'] == sha(p / 'runtime/execution.json') and a['request_sha256'] == inputs['request_sha256'], 'ML_PREVIOUS_ACCEPTANCE_CHANGED')
    if previous:
        latest = load(root / 'latest.json'); retained = load(root / 'host-sessions' / latest['session'] / 'acceptance.json')['lease_retained']
        if retained: require_resume_context(load(root / 'host-sessions' / latest['session'] / 'intent.json'), split, mode)
    g.idle(owner(root) if retained else None); r.trino_queries_idle()
    if not (root / 'staged.json').exists():
        require(not previous, 'ML_STAGING_MISSING_NO_REGENERATION')
        transfer = g.module('dat03-portable-import').TRANSFER.replace('dat03-portable-import', 'dat03-prepared-ml')
        transfer = transfer.replace('controller\\.py|dat03-portable-capacity\\.py', 'controller\\.py|dat03-portable-gold\\.py|feature-scaling\\.json|resource-profile\\.json|dat03-portable-capacity\\.py')
        files = inputs['files'] + [{'name': 'inputs.json', 'bytes': (root / 'inputs.json').stat().st_size, 'sha256': sha(root / 'inputs.json')}]
        with subprocess.Popen(['docker', 'exec', '-i', g.CONTAINER, 'python3', '-c', transfer, inside], stdin=subprocess.PIPE) as child:
            child.stdin.write((json.dumps(files) + '\n').encode())
            for f in files:
                with safe(root / f['name']).open('rb') as stream: shutil.copyfileobj(stream, child.stdin, 1024**2)
            child.stdin.close(); require(child.wait(timeout=120) == 0, 'ML_STAGE_FAILED_NO_RETRY')
        save(root / 'staged.json', {'inputs_sha256': sha(root / 'inputs.json')})
    require(load(root / 'staged.json')['inputs_sha256'] == sha(root / 'inputs.json'), 'ML_STAGED_LIST_CHANGED')
    gate = ''
    if mode != 'preflight':
        accepted = load(root / ('preflight-' + split + '.json')); src = root / 'host-sessions' / accepted['session']
        require(sha(src / 'acceptance.json') == accepted['acceptance_sha256'], 'ML_CAPACITY_ACCEPTANCE_CHANGED')
        value = load(src / 'runtime/report.json', 8 * 1024**2); require(value['status'] == 'PREFLIGHT_PASSED', 'ML_CAPACITY_NOT_PASSED')
        gate = sha(src / 'runtime/report.json')
        send = 'import sys,hashlib;from pathlib import Path;p=Path(sys.argv[1]);raw=sys.stdin.buffer.read(8*1024**2+1);assert len(raw)<=8*1024**2 and hashlib.sha256(raw).hexdigest()==sys.argv[2];assert not any(x.is_symlink() for x in (p,*p.parents));p.parent.mkdir(exist_ok=True);p.open("xb").write(raw) if not p.exists() else None;assert hashlib.sha256(p.read_bytes()).hexdigest()==sys.argv[2]'
        subprocess.run(['docker', 'exec', '-i', g.CONTAINER, 'python3', '-c', send, inside + '/' + split + '/preflight-accepted.json', gate], input=(src / 'runtime/report.json').read_bytes(), check=True, timeout=30)
    session = uuid.uuid4().hex; host = root / 'host-sessions' / session; host.mkdir(parents=True, mode=0o700)
    save(host / 'intent.json', {'session': session, 'split': split, 'mode': mode, 'services_before': before, 'inputs_sha256': sha(root / 'inputs.json'), 'gate_sha256': gate})
    bootstrap = 'import sys,hashlib,runpy;from pathlib import Path;p=Path(sys.argv[1]);assert hashlib.sha256((p/"controller.py").read_bytes()).hexdigest()==sys.argv[2];sys.argv=[str(p/"controller.py"),"worker","--folder",str(p),"--inputs-sha256",sys.argv[3],"--session",sys.argv[4],"--split",sys.argv[5],"--worker-mode",sys.argv[6],"--seconds",sys.argv[7],"--gate-sha256",sys.argv[8]];runpy.run_path(str(p/"controller.py"),run_name="__main__")'
    with (host / 'worker.private').open('xb') as log:
        child = subprocess.Popen(['docker', 'exec', g.CONTAINER, 'python3', '-B', '-X', 'pycache_prefix=/dev/null', '-c', bootstrap, inside, sha(root / 'controller.py'), sha(root / 'inputs.json'), session, split, mode, str(seconds), gate], stdout=log, stderr=subprocess.STDOUT)
        started = time.monotonic(); last = None; stopped = False
        while child.poll() is None:
            require(time.monotonic() - started <= seconds + 180, 'ML_HOST_UNKNOWN_OUTCOME_KEEP_LEASE')
            if shutil.disk_usage(root).free < 4 * 1024**3 and not stopped:
                subprocess.run(['docker', 'exec', g.CONTAINER, 'python3', '-c', 'from pathlib import Path;import sys;Path(sys.argv[1]).write_bytes(b"{}")', inside + '/stop-' + session + '.json'], check=True, timeout=20); stopped = True
            probe = 'import sys,re;from pathlib import Path;p=Path(sys.argv[1]);v=[x for x in p.read_text(errors="replace").splitlines() if re.fullmatch(r"event=prepared_ml_(?:phase|completed) split=(?:REPRODUCTION|EXTENSION) (?:step=[a-z_]+(?: ordinal=[0-9]+/10)?|status=[A-Z_]+)",x)] if p.exists() else [];print(v[-1] if v else "event=prepared_ml_initial_verification")'
            try:
                v = subprocess.run(['docker', 'exec', g.CONTAINER, 'python3', '-c', probe, inside + '/sessions/' + session + '/stdout.private'], capture_output=True, text=True, timeout=20)
                if v.returncode == 0 and v.stdout.strip() != last: last = v.stdout.strip(); print(last, flush=True)
            except subprocess.TimeoutExpired: pass
            time.sleep(15)
        require(child.wait() == 0 and not stopped, 'ML_SESSION_FAILED_KEEP_CHECKPOINT_AND_LEASE')
    g.idle(owner(root)); r.trino_queries_idle(); require(before == r.services(), 'ML_RUNTIME_SERVICES_CHANGED')
    subprocess.run(['docker', 'cp', g.CONTAINER + ':' + inside + '/sessions/' + session, str(host / 'runtime')], check=True, timeout=120)
    c = load(host / 'runtime/candidate.json'); result = load(host / 'runtime/report.json', 8 * 1024**2); execution = load(host / 'runtime/execution.json')
    require(c['report_sha256'] == sha(host / 'runtime/report.json') and c['execution_sha256'] == sha(host / 'runtime/execution.json')
            and execution['exit_code'] == 0 and execution['stop_reason'] is None and result['request_sha256'] == inputs['request_sha256']
            and result['gold_snapshots_unchanged'] is True, 'ML_CAPTURE_CHAIN_CHANGED')
    status = result['status']; require(status in ('PREFLIGHT_PASSED', 'BLOCKED_RESOURCE', 'DATASET_EXPORTED', 'CHECKPOINTED'), 'ML_UNKNOWN_TERMINAL_STATUS')
    if status == 'DATASET_EXPORTED':
        final = result['final_readback']; require(result['completed_steps'] == 10 and final['status'] == 'DATASET_EXPORTED' and final['exported'] is True
            and final['trusted_handoff_verified'] is True and final['quality_gate_status'] == 'PASSED' and final['invalid_feature_count'] == 0, 'ML_FINAL_HANDOFF_NOT_ACCEPTED')
        prior = root / ('exported-' + split + '.json')
        if prior.exists():
            old = load(root / 'host-sessions' / load(prior)['session'] / 'runtime/report.json', 8 * 1024**2)['final_readback']
            for key in ('dataset_id', 'collection_id', 'mainshock_snapshot_id', 'candidate_snapshot_id', 'registry_snapshot_id', 'dataset_manifest_sha256', 'bundle_manifest_sha256', 'checksums_sha256'):
                require(final[key] == old[key], 'ML_RERUN_PINS_OR_BYTES_CHANGED')
            require(final['reused'] is True, 'ML_RERUN_DID_NOT_REUSE')
    release = status != 'CHECKPOINTED'
    if release:
        script = 'import sys,json;sys.path.insert(0,"/opt/airflow/dags");from source_run_guard import lease;p=json.loads(sys.argv[1]);lease("assert",p);print(json.dumps(lease("release",p)))'
        v = subprocess.run(['docker', 'exec', g.CONTAINER, 'python3', '-c', script, json.dumps(owner(root))], capture_output=True, text=True, check=True, timeout=20)
        require(json.loads(v.stdout) == {'status': 'RELEASED', **owner(root)}, 'ML_OWN_RELEASE_UNCONFIRMED')
    g.idle(None if release else owner(root)); r.trino_queries_idle(); require(before == r.services(), 'ML_POSTFLIGHT_SERVICES_CHANGED')
    save(host / 'acceptance.json', {'status': status, 'split': split, 'mode': mode, 'request_sha256': inputs['request_sha256'], 'report_sha256': sha(host / 'runtime/report.json'),
         'execution_sha256': sha(host / 'runtime/execution.json'), 'candidate_sha256': sha(host / 'runtime/candidate.json'), 'lease_retained': not release, 'services_unchanged': True})
    pin = {'session': session, 'acceptance_sha256': sha(host / 'acceptance.json')}
    if mode == 'preflight': save(root / ('preflight-' + split + '.json'), pin)
    if status == 'DATASET_EXPORTED' and not (root / ('exported-' + split + '.json')).exists(): save(root / ('exported-' + split + '.json'), pin)
    # Mutable pointer only; every session/receipt it references remains immutable and SHA-bound.
    (root / 'latest.json').write_text(json.dumps(pin) + '\n')
    print(json.dumps({'event': 'prepared_ml_session_accepted', 'session': session, 'split': split, 'status': status,
        'lease_released': release, 'dataset_id': result['dataset_id'], 'sql_calls': result['sql_calls'], 'blocked_windows': result.get('blocked_windows', [])}), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument('mode', choices=('prepare', 'preflight', 'build', 'rerun', 'worker'))
    p.add_argument('--folder', type=Path, required=True); p.add_argument('--evidence', type=Path); p.add_argument('--evidence-sha256'); p.add_argument('--split', choices=SPLITS)
    p.add_argument('--seconds', type=int, default=6000); p.add_argument('--session'); p.add_argument('--inputs-sha256'); p.add_argument('--worker-mode'); p.add_argument('--gate-sha256', default='')
    p.add_argument('--profile', type=Path); p.add_argument('--profile-sha256')
    a = p.parse_args()
    if a.mode == 'prepare': prepare(a.folder, a.evidence, a.evidence_sha256, a.profile, a.profile_sha256)
    elif a.mode == 'worker': worker(a.folder, a.inputs_sha256, a.split, a.worker_mode, a.session, a.seconds, a.gate_sha256)
    else: run(a.folder, a.split, a.mode, a.seconds)


if __name__ == '__main__': main()
