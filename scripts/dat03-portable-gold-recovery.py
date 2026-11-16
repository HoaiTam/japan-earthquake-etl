#!/usr/bin/env python3
"""One explicit continuation for a diagnosed dimension-column alignment failure, not auto-retry."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import uuid
import zipfile
import hashlib

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('portable_gold', HERE / 'dat03-portable-gold.py')
g = importlib.util.module_from_spec(spec); spec.loader.exec_module(g)
require, sha, load, save, safe = g.require, g.sha, g.load, g.save, g.safe
VERSION = 'dat03-dimension-column-alignment-v1'
CHANGED = {'ie212/earthquake/spark/gold/GoldIcebergWriter.class', 'ie212/earthquake/spark/gold/PortableGoldPublicationJob.class'}
ADDED = {'ie212/earthquake/spark/gold/PreparedGoldContinuation.class'}


def classes(path):
    with zipfile.ZipFile(safe(path)) as jar:
        names = [n for n in jar.namelist() if n.startswith('ie212/earthquake/spark/') and n.endswith('.class')]
        require(names and len(names) == len(set(names)), 'RECOVERY_UNIQUE_ORIGINAL_CLASSES_REQUIRED')
        return {n: hashlib.sha256(jar.read(n)).hexdigest() for n in names}


def compatibility(original, current):
    a, b = classes(original), classes(current)
    require(set(a) <= set(b) and {n for n in a if a[n] != b[n]} == CHANGED
            and set(b) - set(a) == ADDED, 'RECOVERY_CHANGED_TRANSFORM_OR_OTHER_COMPONENT')


def continuation(root, path, digest=None):
    root, _ = g.operation(root); path = safe(path)
    require(re.fullmatch(re.escape(str(root)) + r'/continuations/[a-f0-9]{32}/continuation.json', str(path)), 'EXACT_CONTINUATION_PATH_REQUIRED')
    if digest: require(sha(path) == digest, 'CONTINUATION_MANIFEST_CHANGED')
    c = load(path); request = load(root / 'request.json')
    require(c['version'] == VERSION and c['namespace'] == g.NAMESPACE and c['run_id'] == request['run_id']
            and c['request_sha256'] == sha(root / 'request.json') and c['inputs_sha256'] == sha(root / 'inputs.json')
            and c['original_runner_sha256'] == request['runner_sha256'] == sha(root / 'runner.jar')
            and c['continuation_runner_sha256'] == sha(path.parent / 'runner.jar'), 'RECOVERY_CONTEXT_CHANGED')
    for name, digest in c['scripts'].items(): require(sha(path.parent / name) == digest, 'RECOVERY_SCRIPT_CHANGED')
    failed = root / 'host-sessions' / c['failed_session']
    require(sha(failed / 'failed-runtime/commit-execution.json') == c['failed_execution_sha256']
            and load(failed / 'failed-runtime/commit-execution.json')['exit_code'] == 1
            and not (failed / 'acceptance.json').exists(), 'RECOVERY_FAILED_CHECKPOINT_CHANGED')
    compatibility(root / 'runner.jar', path.parent / 'runner.jar')
    return c


def prepare(root, inspection, expected, failed_session):
    root, _ = g.operation(root); require(re.fullmatch(r'[a-f0-9]{32}', failed_session), 'EXACT_FAILED_SESSION_REQUIRED')
    require(sha(inspection) == expected, 'PINNED_READ_ONLY_INSPECTION_REQUIRED'); v = load(inspection)
    request = load(root / 'request.json'); failed = root / 'host-sessions' / failed_session
    execution = load(failed / 'failed-runtime/commit-execution.json')
    require(execution['exit_code'] == 1 and execution['stop_reason'] is None and execution['phase'] == 'commit'
            and not (failed / 'failed-runtime/candidate.json').exists(), 'DIAGNOSED_COMMIT_FAILURE_REQUIRED')
    error = (failed / 'failed-runtime/commit-stderr.private').read_text(errors='replace')
    require('INCOMPATIBLE_COLUMN_TYPE' in error and 'EXCEPT' in error and 'DATE' in error and 'INT' in error, 'DIMENSION_COLUMN_ALIGNMENT_FAILURE_REQUIRED')
    require(v['data_mutations'] is False and v['namespace'] == g.NAMESPACE and v['request_sha256'] == sha(root / 'request.json')
            and v['run_id'] == request['run_id'] and set(v['before_snapshots']) == set(request['expected_counts']), 'EXACT_PARTIAL_INSPECTION_REQUIRED')
    require(all(isinstance(x, str) and re.fullmatch(r'0|[1-9][0-9]*', x) for x in v['before_snapshots'].values()), 'EXACT_STRING_INT64_PINS_REQUIRED')
    require(all(v['before_snapshots'][n] != '0' for n in ('event_current', 'event_source_bridge'))
            and all(v['before_snapshots'][n] == '0' for n in v['before_snapshots'] if n not in ('event_current', 'event_source_bridge')), 'EXACT_TWO_FACT_PARTIAL_STATE_REQUIRED')
    require(all(v['snapshot_metadata'][n]['identity_sha256'] == v['original_identity_sha256']
                and v['snapshot_metadata'][n]['operation_id'] == request['run_id']
                and v['snapshot_metadata'][n]['count'] == request['expected_counts'][n]
                for n in ('event_current', 'event_source_bridge')), 'PARTIAL_FACT_OWNERSHIP_REQUIRED')
    runner = HERE.parent / 'spark/target/japan-earthquake-etl-runner.jar'; compatibility(root / 'runner.jar', runner)
    g.idle(g.owner(request)); r = g.module('research-rollout'); r.trino_queries_idle()
    require(r.services() == load(failed / 'intent.json')['services_before'], 'RECOVERY_ORIGINAL_SERVICES_CHANGED')
    folder = root / 'continuations' / uuid.uuid4().hex; folder.mkdir(parents=True, mode=0o700)
    for source, name in ((runner, 'runner.jar'), (Path(__file__), 'controller.py'), (HERE / 'dat03-portable-gold.py', 'dat03-portable-gold.py'),
                         (HERE / 'dat03-portable-capacity.py', 'dat03-portable-capacity.py')):
        shutil.copyfile(source, folder / name); os.chmod(folder / name, 0o600)
    save(folder / 'continuation.json', {'version': VERSION, 'run_id': request['run_id'], 'namespace': g.NAMESPACE,
         'request_sha256': sha(root / 'request.json'), 'inputs_sha256': sha(root / 'inputs.json'),
         'original_runner_sha256': request['runner_sha256'], 'continuation_runner_sha256': sha(folder / 'runner.jar'),
         'failed_session': failed_session, 'failed_execution_sha256': sha(failed / 'failed-runtime/commit-execution.json'),
         'inspection_sha256': expected, 'original_identity_raw_sha256': v['original_identity_raw_sha256'],
         'before_snapshots': v['before_snapshots'], 'scripts': {n: sha(folder / n) for n in ('controller.py', 'dat03-portable-gold.py', 'dat03-portable-capacity.py')}})
    print(json.dumps({'continuation': str(folder / 'continuation.json'), 'sha256': sha(folder / 'continuation.json'), 'writes': False}), flush=True)


def finalize(root, path, digest, session):
    c = continuation(root, path, digest); failed = root / 'host-sessions' / c['failed_session']; host = root / 'host-sessions' / session
    a = load(host / 'acceptance.json'); candidate = load(host / 'runtime/candidate.json', 4 * 1024**2)
    require(a['status'] == 'PUBLISHED' and a['mode'] == 'recover' and a['candidate_sha256'] == sha(host / 'runtime/candidate.json')
            and a['request_sha256'] == c['request_sha256'] and a['source_lease_empty'] is True, 'ACCEPTED_RECOVERY_REQUIRED')
    for record in candidate['records']: require(record['result']['continuation_sha256'] == digest, 'RECOVERY_RESULT_CODE_PIN_CHANGED')
    pins = candidate['records'][-1]['result']['snapshots']
    for n, pin in c['before_snapshots'].items():
        if pin != '0': require(str(pins[n]['snapshot_id']) == pin, 'RECOVERY_REWROTE_COMMITTED_FACT')
    save(failed / 'resolution.json', {'version': VERSION, 'request_sha256': c['request_sha256'], 'continuation_sha256': digest,
         'failed_execution_sha256': c['failed_execution_sha256'], 'recovery_session': session,
         'recovery_acceptance_sha256': sha(host / 'acceptance.json'), 'before_snapshots': c['before_snapshots'],
         'failed_evidence_preserved': True, 'old_failure_marked_success': False})


def run(root, path, digest, seconds):
    root, inside = g.operation(root); c = continuation(root, path, digest); request = load(root / 'request.json')
    require(type(seconds) is int and 600 <= seconds <= 6000, 'BOUNDED_RECOVERY_SESSION_REQUIRED')
    require(shutil.disk_usage(root).free >= load(root / 'inputs.json')['physical_start_required_bytes'], 'RECOVERY_DISK_CAPACITY_REQUIRED')
    r = g.module('research-rollout'); before = r.services(); g.idle(g.owner(request)); r.trino_queries_idle()
    require(before == load(root / 'host-sessions' / c['failed_session'] / 'intent.json')['services_before'], 'RECOVERY_SERVICES_CHANGED')
    require(not (path.parent / 'staged.json').exists() and not (root / 'first-published.json').exists(), 'RECOVERY_NO_AUTOMATIC_RETRY')
    for folder in (root / 'host-sessions').iterdir():
        require(folder.name == c['failed_session'] or (folder / 'acceptance.json').is_file(), 'OTHER_UNACCEPTED_SESSION')
    remote = inside + '/continuations/' + path.parent.name
    transfer = g.module('dat03-portable-import').TRANSFER.replace('dat03-portable-import/[a-f0-9]{32}', 'dat03-portable-gold/[a-f0-9]{32}/continuations/[a-f0-9]{32}')
    transfer = transfer.replace('controller\\.py|dat03-portable-capacity\\.py', 'controller\\.py|dat03-portable-gold\\.py|continuation\\.json|dat03-portable-capacity\\.py')
    files = [{'name': n, 'bytes': (path.parent / n).stat().st_size, 'sha256': sha(path.parent / n)} for n in
             ('runner.jar', 'controller.py', 'dat03-portable-gold.py', 'dat03-portable-capacity.py', 'continuation.json')]
    with subprocess.Popen(['docker', 'exec', '-i', g.CONTAINER, 'python3', '-c', transfer, remote], stdin=subprocess.PIPE) as child:
        child.stdin.write((json.dumps(files) + '\n').encode())
        for f in files:
            with safe(path.parent / f['name']).open('rb') as stream: shutil.copyfileobj(stream, child.stdin, 1024**2)
        child.stdin.close(); require(child.wait(timeout=120) == 0, 'RECOVERY_STAGE_FAILED_KEEP_CHECKPOINT')
    save(path.parent / 'staged.json', {'continuation_sha256': digest})
    session = uuid.uuid4().hex; host = root / 'host-sessions' / session; host.mkdir(mode=0o700)
    save(host / 'intent.json', {'session': session, 'mode': 'recover', 'inputs_sha256': sha(root / 'inputs.json'),
         'services_before': before, 'continuation_sha256': digest})
    bootstrap = 'import hashlib,sys,runpy;from pathlib import Path;p=Path(sys.argv[1]);assert hashlib.sha256((p/"controller.py").read_bytes()).hexdigest()==sys.argv[2];sys.argv=[str(p/"controller.py"),"worker","--folder",sys.argv[3],"--continuation",str(p/"continuation.json"),"--continuation-sha256",sys.argv[4],"--session",sys.argv[5],"--seconds",sys.argv[6]];runpy.run_path(str(p/"controller.py"),run_name="__main__")'
    with (host / 'worker.private').open('xb') as log:
        child = subprocess.Popen(['docker', 'exec', g.CONTAINER, 'python3', '-c', bootstrap, remote, c['scripts']['controller.py'], inside, digest, session, str(seconds)], stdout=log, stderr=subprocess.STDOUT)
        started = time.monotonic(); last = None; stopped = False
        while child.poll() is None:
            require(time.monotonic() - started <= seconds + 180, 'RECOVERY_UNKNOWN_OUTCOME_KEEP_LEASE')
            if shutil.disk_usage(root).free < 4 * 1024**3 and not stopped:
                subprocess.run(['docker', 'exec', g.CONTAINER, 'python3', '-c', 'from pathlib import Path;import sys;Path(sys.argv[1]).write_bytes(b"{}")', inside + '/stop-' + session + '.json'], check=True, timeout=20); stopped = True
            probe = 'import re,sys;from pathlib import Path;p=Path(sys.argv[1]);lines=[]\nfor phase in ("commit","publication"):\n f=p/(phase+"-stdout.private")\n if f.exists():lines.extend(x for x in f.read_text(errors="replace").splitlines() if re.fullmatch(r"event=prepared_gold_[a-z_]+ run_id=dat03-gold-[a-f0-9]{32} (?:phase=[a-z]+ query=[0-9]+/[0-9]+|table=[a-z_]+|phase=[a-z]+ status=[A-Z_]+)",x))\nprint(lines[-1] if lines else "event=prepared_gold_recovery_initial_verification")'
            try:
                v = subprocess.run(['docker', 'exec', g.CONTAINER, 'python3', '-c', probe, inside + '/sessions/' + session], capture_output=True, text=True, timeout=20)
                if v.returncode == 0 and v.stdout.strip() != last: last = v.stdout.strip(); print(last, flush=True)
            except subprocess.TimeoutExpired: pass
            time.sleep(15)
        require(child.wait() == 0 and not stopped, 'RECOVERY_FAILED_KEEP_CHECKPOINT_AND_LEASE')
    g.capture(root, session); finalize(root, path, digest, session)


def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument('mode', choices=('prepare', 'run', 'worker', 'finalize'))
    p.add_argument('--folder', type=Path, required=True); p.add_argument('--inspection', type=Path); p.add_argument('--inspection-sha256'); p.add_argument('--failed-session')
    p.add_argument('--continuation', type=Path); p.add_argument('--continuation-sha256'); p.add_argument('--seconds', type=int, default=6000); p.add_argument('--session')
    a = p.parse_args()
    if a.mode == 'prepare': prepare(a.folder, a.inspection, a.inspection_sha256, a.failed_session)
    elif a.mode == 'run': run(a.folder, a.continuation, a.continuation_sha256, a.seconds)
    elif a.mode == 'finalize': finalize(a.folder, a.continuation, a.continuation_sha256, a.session)
    else:
        c = load(a.continuation); require(sha(a.continuation) == a.continuation_sha256, 'WORKER_CONTINUATION_CHANGED')
        for name, digest in c['scripts'].items(): require(sha(a.continuation.parent / name) == digest, 'WORKER_SCRIPT_CHANGED')
        g.worker(a.folder, c['inputs_sha256'], a.session, 'recover', a.seconds, continuation=a.continuation, continuation_sha=a.continuation_sha256)


if __name__ == '__main__': main()
