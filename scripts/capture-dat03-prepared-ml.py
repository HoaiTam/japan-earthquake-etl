#!/usr/bin/env python3
"""Read-only capture of exact indexed portable files from an accepted official ML session.

No source/warehouse copies, lifecycle changes, semantic loader claims, or automatic retries.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

spec = importlib.util.spec_from_file_location('prepared_ml', Path(__file__).resolve().parent / 'dat03-prepared-ml.py')
a = importlib.util.module_from_spec(spec); spec.loader.exec_module(a)

REMOTE_INDEX = r'''
import hashlib,json,sys,re
from pathlib import Path
p=Path(sys.argv[1]);pins=json.loads(sys.argv[2])
assert not any(q.is_symlink() for q in (p,*p.parents))
assert p.is_dir()
for name,pin in [('dataset_manifest.json',pins['bundle_manifest_sha256']),('checksums.sha256',pins['checksums_sha256'])]:
 f=p/name;assert f.is_file() and not f.is_symlink() and 0<f.stat().st_size<=4*1024**2 and hashlib.sha256(f.read_bytes()).hexdigest()==pin
files={}
for line in (p/'checksums.sha256').read_text(encoding='ascii').splitlines():
 assert re.fullmatch(r'[a-f0-9]{64}  (?:dataset_manifest\.json|collection-preparation\.json|export-checksums\.sha256|shard_[0-9]{5}/(?:features|mainshocks)\.parquet)',line)
 digest,name=line.split('  ');assert name not in files
 f=p/name;assert not any(q.is_symlink() for q in (f,*f.parents)) and f.is_file() and 0<f.stat().st_size<=64*1024**2
 files[name]={'bytes':f.stat().st_size,'sha256':digest}
assert {'dataset_manifest.json','collection-preparation.json','export-checksums.sha256'}<=set(files)
files['checksums.sha256']={'bytes':(p/'checksums.sha256').stat().st_size,'sha256':pins['checksums_sha256']}
assert len(files)<=132 and sum(f['bytes'] for f in files.values())<=8*1024**3
assert {f.relative_to(p).as_posix() for f in p.rglob('*') if not f.is_dir()}==set(files)
print(json.dumps(files))
'''


def validate_session(root, session, expected, split):
    a.require(split in a.SPLITS and re.fullmatch(r'[a-f0-9]{32}', session or ''), 'EXACT_EXPORTED_SESSION_REQUIRED')
    p = root / 'host-sessions' / session
    a.require(a.sha(p / 'acceptance.json') == expected, 'ML_EXTERNAL_ACCEPTANCE_CHANGED')
    accepted = a.load(p / 'acceptance.json'); report = a.load(p / 'runtime/report.json', 8*1024**2)
    execution = a.load(p / 'runtime/execution.json'); candidate = a.load(p / 'runtime/candidate.json')
    a.require(accepted['status'] == 'DATASET_EXPORTED' and accepted['lease_retained'] is False
              and accepted['report_sha256'] == a.sha(p / 'runtime/report.json')
              and accepted['candidate_sha256'] == a.sha(p / 'runtime/candidate.json')
              and accepted['execution_sha256'] == a.sha(p / 'runtime/execution.json')
              and accepted['request_sha256'] == a.sha(root / 'request.json'), 'ML_CAPTURE_ACCEPTANCE_CHANGED')
    a.require(execution['exit_code'] == 0 and execution['stop_reason'] is None
              and candidate['report_sha256'] == accepted['report_sha256'] and candidate['execution_sha256'] == accepted['execution_sha256']
              and candidate['request_sha256'] == accepted['request_sha256'] and candidate['split'] == split
              and report['request_sha256'] == accepted['request_sha256'] and report['status'] == 'DATASET_EXPORTED'
              and report['dataset_split'] == split and report['completed_steps'] == 10 and report['gold_snapshots_unchanged'] is True,
              'ML_COMPLETE_EXPORTED_SPLIT_REQUIRED')
    final = report['final_readback']
    a.require(final['status'] == 'DATASET_EXPORTED' and final['exported'] is True and final['trusted_handoff_verified'] is True
              and final['quality_gate_status'] == 'PASSED' and final['invalid_feature_count'] == 0
              and final['dataset_id'] == report['dataset_id'] and re.fullmatch(r'ds_[a-f0-9]{64}', final['dataset_id']), 'ML_TRUSTED_HANDOFF_REQUIRED')
    return final


def capture(root, session, expected, split, output):
    root, inside = a.operation(root); output = a.safe(output)
    a.require(not output.exists() and output.parent.is_dir() and not output.is_relative_to(root), 'NEW_CAPTURE_ROOT_OUTSIDE_OPERATION_REQUIRED')
    pins = validate_session(root, session, expected, split)
    r = a.g.module('research-rollout'); before = r.services(); a.g.idle(None); r.trino_queries_idle()
    source = inside + '/' + split + '/export/dataset_id=' + pins['dataset_id']
    query = subprocess.run(['docker', 'exec', a.g.CONTAINER, 'python3', '-B', '-c', REMOTE_INDEX, source, json.dumps(pins)],
                           check=True, capture_output=True, text=True, timeout=30)
    files = json.loads(query.stdout); total = sum(f['bytes'] for f in files.values())
    a.require(shutil.disk_usage(output.parent).free >= 4*1024**3 + 2*total, 'ML_CAPTURE_PHYSICAL_RESERVE_REQUIRED')
    output.mkdir(mode=0o700)
    for name, pin in files.items():
        target = output / name; target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        subprocess.run(['docker', 'cp', a.g.CONTAINER + ':' + source + '/' + name, str(target)], check=True, timeout=120)
        a.require(a.safe(target).is_file() and target.stat().st_size == pin['bytes'] and a.sha(target) == pin['sha256'], 'ML_CAPTURE_BYTES_CHANGED_KEEP_EVIDENCE')
        os.chmod(target, 0o600)
    a.g.idle(None); r.trino_queries_idle(); a.require(r.services() == before, 'ML_CAPTURE_SERVICES_CHANGED')
    result = {'status': 'PORTABLE_CAPTURED_BYTES_VERIFIED', 'session': session, 'dataset_split': split,
              'acceptance_sha256': expected, 'directory': str(output), 'files': len(files), 'bytes': total,
              'dataset_id': pins['dataset_id'], 'manifest_sha256': pins['bundle_manifest_sha256'], 'checksums_sha256': pins['checksums_sha256'],
              'semantic_loader_verified': False, 'scientific_approval': False, 'source_lease_empty': True, 'services_unchanged': True}
    a.save(output.parent / (output.name + '-capture.json'), result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__); p.add_argument('--folder', type=Path, required=True)
    p.add_argument('--session', required=True); p.add_argument('--acceptance-sha256', required=True); p.add_argument('--split', choices=a.SPLITS, required=True)
    p.add_argument('--output', type=Path, required=True); v = p.parse_args(); capture(v.folder,v.session,v.acceptance_sha256,v.split,v.output)
